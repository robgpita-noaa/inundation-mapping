#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

Command Line Usage:
    r=<dem_resolution>; /foss_fim/data/usgs/lidar_dems_from_service.py -r ${r} -t /data/misc/lidar/ngwpc_PI1_lidar_tiles_${r}m.gpkg -d /data/inputs/3dep_dems/${r}m_5070_lidar_wms -o
"""

from __future__ import annotations
from typing import List, Set
from numbers import Number
from pyproj import CRS
from shapely.geometry import Polygon, MultiPolygon

import warnings
import shutil
import argparse
import os
from pathlib import Path
from functools import partial
import time

from osgeo import gdal
from rasterio.enums import Resampling
import dask
import numpy as np
import pandas as pd
import geopandas as gpd
import py3dep
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client
from tqdm import tqdm


from utils.shared_variables import elev_raster_ndv as ELEV_RASTER_NDV

# Enable exceptions for GDAL
gdal.UseExceptions()

# get directories from env variables
projectDir = os.getenv('projectDir')
srcDir = os.getenv('srcDir')
inputsDir = os.getenv('inputsDir')

# load env variables
load_dotenv(os.path.join(srcDir, 'bash_variables.env'))

# WBD variables
WBD_BUFFER = 5000 # should match buffer size in config file. in CRS horizontal units
WBD_SIZE = 8 # huc size
WBD_FILE = os.path.join(inputsDir, 'wbd', 'WBD_National_EPSG_5070.gpkg') # env file????

# default CRS
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

# computational and process variables
MAX_RETRIES = 25 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client
DEFAULT_SLEEP_TIME = 0 # default sleep time in seconds for retries

WRITE_KWARGS = {
    'driver' : 'GTiff',
    'dtype' : 'float32',
    'windowed' : True,
    'compute' : True,
    'overwrite' : True,
    'blockxsize' : 128,
    'blockysize' : 128,
    'tiled' : True,
    'compress' : 'lzw',
    'BIGTIFF' : 'IF_SAFER',
    'RESAMPLING' : 'bilinear',
    'OVERVIEW_RESAMPLING' : 'bilinear',
    'OVERVIEWS' : 'AUTO',
    'OVERVIEW_COUNT' : 5,
    'OVERVIEW_COMPRESS' : 'LZW',
}

# HyRiver caching variables
# see https://docs.hyriver.io/readme/async-retriever.html
os.environ["HYRIVER_CACHE_DISABLE"] = "true" # disable cache

def retrieve_process_write_single_3dep_dem_tile(
    idx : int,
    huc : str,
    geometry : Polygon | MultiPolygon,
    dem_resolution : Number,
    crs : str | CRS,
    ndv : Number,
    dem_3dep_dir : str,
    write_kwargs : dict,
    write_ext : str,
    sleep_time : int,
    max_retries : int
) -> str:
    """
    Retrieves and processes a single 3DEP DEM tile.
    """

    retries = 0
    while True:
        try:
            # Primary operation
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                dem = py3dep.get_map('DEM', geometry=geometry, resolution=dem_resolution, geo_crs=crs)
        except Exception as e:
            retries += 1
            print(f'Dynamic DEM Failed: {e} - idx: {idx} | retries: {retries}')

            # If primary operation fails after max retries, try fallback option
            if retries > max_retries:
                try:
                    print(f'Using Static DEM - idx: {idx}')
                    dem = py3dep.static_3dep_dem(geometry=geometry, crs=crs, resolution=10)
                except Exception as e:
                    print(f'Static DEM Failed: {e} - idx: {idx}')
                    break
                else:
                    acquired_datetime_utc = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    break
            else:
                print(f'Retrying in {sleep_time} seconds - idx: {idx}')
                time.sleep(sleep_time)
                continue
        else:
            acquired_datetime_utc = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            break

    # raise if no data value is not NaN
    if not np.isnan(dem.rio.nodata):
        raise ValueError(f'No data value is not NaN: {dem.rio.nodata}. {idx}_{huc}')

    # reproject, remove nan padding, and set encoded ndv
    dem = (
        dem
        .odc.reproject( 
            crs,
            resolution=dem_resolution,
            resampling=Resampling.bilinear
        )
        .dropna('y',how='all')
        .dropna('x', how='all')
        .rio.write_nodata(ndv, inplace=True, encoded=True)
    )

    # set attributes
    tile_id = f'{huc}_{idx}'
    dem.attrs['TILE_ID'] = tile_id
    dem.attrs['HUC'] = huc
    dem.attrs['ACQUIRED_DATETIME_UTC'] = acquired_datetime_utc
    
    # create write path
    dem_file_name = os.path.join(dem_3dep_dir,f'{tile_id}.{write_ext}')

    # write file
    dem.rio.to_raster(
        dem_file_name,
        **write_kwargs
    )

    # close dem
    dem.close()

    return dem_file_name


def acquired_and_process_lidar_dems(
    tile_inputs_file : str | Path,
    dem_resolution : Number,
    dem_3dep_dir : str | Path,
    write_kwargs : dict = WRITE_KWARGS,
    write_ext : str = 'tif',
    waive_overwrite_check : bool = False,
    crs : str | CRS = DEFAULT_FIM_PROJECTION_CRS,
    ndv : Number = ELEV_RASTER_NDV,
    client : dask.distributed.Client | None = None,
    num_workers : int = NUM_WORKERS,
    sleep_time : int = DEFAULT_SLEEP_TIME,
    max_retries : int = MAX_RETRIES
) -> Path:
    """
    Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

    Parameters
    ----------
    tile_inputs_file : str | Path
        Path to tile inputs vector file.
    dem_resolution : Number
        DEM resolution in meters.
    dem_3dep_dir : str | Path
        Path to 3DEP DEM directory.
    write_kwargs : dict, default = WRITE_KWARGS
        Write kwargs.
    write_ext : str, default = 'tif'
        Write extension.
    waive_overwrite_check : bool, default = False
        Waive overwrite check.
    ndv : Number, default = ELEV_RASTER_NDV
        No data value.
    client : dask.distributed.Client | None, default = None
        Dask client.
    num_workers : int, default = NUM_WORKERS
        Number of workers.
    sleep_time : int, default = DEFAULT_SLEEP_TIME
        Sleep time in seconds for retries.
    max_retries : int, default = MAX_RETRIES
        Max retries.

    Returns
    -------
    Path
        Path to VRT file.

    Raises
    ------
    ValueError
        If client is not provided and cannot be created.
    """

    # handle retry and overwrite check logic
    if waive_overwrite_check:
        shutil.rmtree(dem_3dep_dir, ignore_errors=True)
    else:
        answer = input('Are you sure you want to overwrite the existing 3DEP DEMs? (yes/y/no/n): ')
        if (answer.lower() == 'y') | (answer.lower() == 'yes'):
            # logger.info('Exiting due to not overwriting.')
            shutil.rmtree(dem_3dep_dir, ignore_errors=True)
        else:
            # logger.info('Exiting due to not overwriting and not retrying.')
            exit()
        
    
    # directory location
    os.makedirs(dem_3dep_dir, exist_ok=True)

    # create dask client
    if client is None:
        try:
            client = get_client()
        except ValueError:
            client = Client(n_workers=num_workers, threads_per_worker=1)
            local_client = True
        else:
            local_client = False

    # load tile inputs
    tile_inputs = gpd.read_file(tile_inputs_file)

    # reproject to EPSG:4326
    #tile_inputs = tile_inputs.to_crs('EPSG:4326')

    # make tile directory
    dem_tile_dir = os.path.join(dem_3dep_dir, 'tiles')
    os.makedirs(dem_tile_dir, exist_ok=True)

    # create partial function for 3dep acquisition
    retrieve_process_write_single_3dep_dem_tile_partial = partial(
        retrieve_process_write_single_3dep_dem_tile,
        dem_resolution = dem_resolution,
        crs = crs,
        ndv = ndv,
        dem_3dep_dir = dem_tile_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext,
        sleep_time = sleep_time,
        max_retries = max_retries
    )

    # Assuming tile_inputs is your GeoDataFrame
    tile_inputs_list = tile_inputs.itertuples(index=False, name=None)
    
    # for debugging
    #tile_inputs_list = list(tile_inputs_list)[:10]

    # download tiles
    '''
    dem_tile_file_names = [None] * len(tile_inputs)
    for i, (idx, huc, tile_geom) in tqdm(
        tile_inputs.iterrows(), total=len(tile_inputs), desc="Downloading 3DEP DEMs by tile"
    ):
        try:
            dem_tile_file_names[i] =  retrieve_process_write_single_3dep_dem_tile_partial(idx, huc, tile_geom)
        except Exception as e:
            pass
    breakpoint()
    '''
    
    # submit futures
    futures = [client.submit(retrieve_process_write_single_3dep_dem_tile_partial, *row) for row in tile_inputs_list]

    dem_tile_file_names = [None] * len(futures)
    with tqdm(total=len(futures), desc=f"Downloading 3DEP DEMs at {dem_resolution}m") as pbar:
        for i, future in enumerate(as_completed(futures)):
            dem_tile_file_names[i] = future.result()  # You can use the result here if needed
            pbar.update(1)

    # create vrt
    opts = gdal.BuildVRTOptions( 
        xRes=dem_resolution,
        yRes=dem_resolution,
        srcNodata=ndv,
        VRTNodata=ndv,
        resampleAlg='bilinear',
        callback=gdal.TermProgress_nocb
    )
    
    destVRT = os.path.join(dem_3dep_dir,f'dem_3dep_{dem_resolution}m.vrt')
    
    if os.path.exists(destVRT):
        os.remove(destVRT)
    
    print(f"Buidling VRT: {destVRT}")
    vrt = gdal.BuildVRT(destName=destVRT, srcDSOrSrcDSTab=dem_tile_file_names, options=opts)
    vrt = None

    # close client
    if local_client:
        client.close()
    
    return destVRT


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Acquires and preprocesses 3DEP DEMs for use with HAND FIM.')


    parser.add_argument(
        '-t', '--tile-inputs-file',
        help='Path to tile inputs file',
        type=str,
        required=True
    )

    parser.add_argument(
        '-r', '--dem-resolution',
        help='DEM resolution in meters',
        type=int,
        required=True
    )

    parser.add_argument(
        '-d', '--dem-3dep-dir',
        help='Path to 3DEP DEM directory',
        type=str,
        required=True
    )

    parser.add_argument(
        '-w', '--write-kwargs',
        help='Write kwargs',
        type=dict,
        default=WRITE_KWARGS,
        required=False
    )

    parser.add_argument(
        '-e', '--write-ext',
        help='Write extension',
        type=str,
        default='tif',
        required=False
    )

    parser.add_argument(
        '-o', '--waive-overwrite-check',
        help='Waive overwrite check',
        default=False,
        action='store_true',
        required=False
    )

    parser.add_argument(
        '-c', '--crs',
        help='Target desired CRS',
        type=str,
        default=DEFAULT_FIM_PROJECTION_CRS,
        required=False
    )

    parser.add_argument(
        '-n', '--ndv',
        help='NDV',
        type=float,
        default=ELEV_RASTER_NDV,
        required=False
    )

    parser.add_argument(
        '-j', '--num-workers',
        help='Number of workers',
        type=int,
        default=NUM_WORKERS,
        required=False
    )

    parser.add_argument(
        '-s', '--sleep-time',
        help='Sleep time in seconds for retries',
        type=int,
        default=DEFAULT_SLEEP_TIME,
        required=False
    )

    parser.add_argument(
        '-m', '--max-retries',
        help='Max retries',
        type=int,
        default=MAX_RETRIES,
        required=False
    )
    
    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    acquired_and_process_lidar_dems(**kwargs)