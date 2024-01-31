#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

Command Line Usage:
    /foss_fim/data/usgs/lidar_dems_from_service.py -t /data/misc/lidar/ngwpc_PI1_lidar_tiles_10m.gpkg -r 10 -d /data/inputs/3dep_dems/10m_5070_py3dep_refactor
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
import itertools

from osgeo import gdal
from rasterio.enums import Resampling
from tqdm.dask import TqdmCallback
import dask
import numpy as np
import pandas as pd
import geopandas as gpd
import py3dep
from shapely import Polygon, MultiPolygon
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

# computational and process variables
MAX_RETRIES = 5 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client

WRITE_KWARGS = {
    'driver' : 'COG',
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
    'PREDICTOR' : "YES",
    'OVERVIEW_PREDICTOR' : "YES"
}

# HyRiver caching variables
# see https://docs.hyriver.io/readme/async-retriever.html
os.environ["HYRIVER_CACHE_DISABLE"] = "true" # disable cache

def retrieve_process_write_single_3dep_dem_tile(
    idx : int,
    huc : str,
    geometry : Polygon | MultiPolygon,
    dem_resolution : Number,
    crs : str,
    ndv : Number,
    dem_3dep_dir : str,
    write_kwargs : dict,
    write_ext : str,
) -> str:
    """
    Retrieves and processes a single 3DEP DEM tile.
    """

    # unpack input data
    #idx, huc, geometry = in_data

    retries = 0
    while True:
        try:
            # Primary operation
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                dem = py3dep.get_map('DEM', geometry=geometry, resolution=dem_resolution, geo_crs=crs)
        except Exception as e:
            print(f'Dynamic DEM Failed: {e} - idx: {idx} | retries: {retries}')
            retries += 1

            # If primary operation fails after max retries, try fallback option
            if retries > MAX_RETRIES:
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
                continue
        else:
            acquired_datetime_utc = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            break


    # reproject and resample
    dem = dem.odc.reproject( 
        crs,
        resolution=dem_resolution,
        resampling=Resampling.bilinear
    )

    # reset ndv to project value
    # existing ndv isclose to -3.4028234663852886e+38
    existing_ndv = -3.4028234663852886e+38
    dem.values[np.isclose(dem, existing_ndv)] = ndv

    dem.rio.write_nodata(ndv, inplace=True)

    # set attributes
    tile_id = f'{huc}_{idx}'
    dem.attrs['TILE_ID'] = tile_id
    dem.attrs['HUC'] = huc
    dem.attrs['ACQUIRED_DATETIME_UTC'] = acquired_datetime_utc
    
    # create write path
    dem_file_name = os.path.join(dem_3dep_dir,f'{tile_id}.{write_ext}')

    # write file
    dem = dem.rio.to_raster(
        dem_file_name,
        **write_kwargs
    )

    return dem_file_name


def acquired_and_process_lidar_dems(
    tile_inputs_file : str | Path,
    dem_resolution : Number,
    dem_3dep_dir : str | Path,
    write_kwargs : dict = WRITE_KWARGS,
    write_ext : str = 'cog',
    waive_overwrite_check : bool = False,
    ndv : Number = ELEV_RASTER_NDV,
    client : dask.distributed.Client | None = None,
    num_workers : int = NUM_WORKERS
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
    write_ext : str, default = 'cog'
        Write extension.
    waive_overwrite_check : bool, default = False
        Waive overwrite check.
    ndv : Number, default = ELEV_RASTER_NDV
        No data value.
    client : dask.distributed.Client | None, default = None
        Dask client.
    num_workers : int, default = NUM_WORKERS
        Number of workers.

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
    if (not waive_overwrite_check) & os.path.exists(dem_3dep_dir):
        answer = input('Are you sure you want to overwrite the existing 3DEP DEMs? (yes/y/no/n): ')
        if (answer.lower() == 'y') | (answer.lower() == 'yes'):
            # logger.info('Exiting due to not overwriting.')
            shutil.rmtree(dem_3dep_dir)
        else:
            # logger.info('Exiting due to not overwriting and not retrying.')
            exit()
    elif os.path.exists(dem_3dep_dir):
        shutil.rmtree(dem_3dep_dir)
    
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

    # make tile directory
    dem_tile_dir = os.path.join(dem_3dep_dir, 'tiles')
    os.makedirs(dem_tile_dir, exist_ok=True)

    # create partial function for 3dep acquisition
    retrieve_process_write_single_3dep_dem_tile_partial = partial(
        retrieve_process_write_single_3dep_dem_tile,
        dem_resolution = dem_resolution,
        crs = tile_inputs.crs,
        ndv = ndv,
        dem_3dep_dir = dem_tile_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext
    )

    # download tiles
    #dem_tile_file_names = [None] * len(tile_inputs)
    #for i, (idx, huc, tile_geom) in tqdm(
    #    tile_inputs.iterrows(), total=len(tile_inputs), desc="Downloading 3DEP DEMs by tile"
    #):
        #dem_tile_file_names[i] =  retrieve_process_write_single_3dep_dem_tile_partial(idx, huc, tile_geom)

    # Assuming tile_inputs is your GeoDataFrame
    tile_inputs_list = tile_inputs.itertuples(index=False, name=None)
    
    # for debugging
    #tile_inputs_list = list(tile_inputs_list)[:10]
    
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
        default='cog',
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
    
    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    acquired_and_process_lidar_dems(**kwargs)