#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

Command Line Usage:
    /foss_fim/data/usgs/acquire_and_preprocess_lidar_dems.py -r 30 -d /data/inputs/3dep_dems/10m_test -w /data/misc/lidar/ngwpc_PI1_lidar_hucs.gpkg -l ngwpc_PI1_lidar_hucs
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

from osgeo.gdal import BuildVRT, BuildVRTOptions
from rasterio.enums import Resampling
from tqdm.dask import TqdmCallback
import dask
import numpy as np
import pandas as pd
import py3dep
from shapely import Polygon, MultiPolygon
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client
from tqdm import tqdm


from utils.shared_variables import elev_raster_ndv as ELEV_RASTER_NDV

# get directories from env variables
projectDir = os.getenv('projectDir')
srcDir = os.getenv('srcDir')
inputsDir = os.getenv('inputsDir')

# load env variables
load_dotenv(os.path.join(srcDir, 'bash_variables.env'))

PREP_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

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

    retries = 1
    dem_source = "failed"
    while True:
        try:
            # Primary operation
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                dem = py3dep.get_map('DEM', geometry=geometry, resolution=dem_resolution, geo_crs=crs)
            dem_source = "dynamic"
            acquired_datetime_utc = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            break
        except Exception as e:
            print(f'Dynamic DEM Failed: {e} - idx: {idx} | retries: {retries}')
            retries += 1

        # If primary operation fails after max retries, try fallback option
        if retries > MAX_RETRIES:
            try:
                print(f'Using Static DEM - idx: {idx} | retries: {retries}')
                dem = py3dep.static_3dep_dem(geometry=geometry, crs=crs, resolution=10)
                dem_source = "static"
                acquired_datetime_utc = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                break
            except Exception as e:
                print(f'Static DEM Failed: {e} - idx: {idx}')

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


def Acquired_and_process_lidar_dems(
    tile_inputs_file : str | Path,
    dem_resolution : Number,
    dem_3dep_dir : str | Path,
    write_kwargs : dict = WRITE_KWARGS,
    write_ext : str = 'cog',
    crs : str | int | CRS = PREP_CRS,
    ndv : Number = ELEV_RASTER_NDV,
    num_workers : int = NUM_WORKERS,
    client : dask.distributed.Client | None = None,
    overwrite_check : bool = False
) -> Path:

    # handle retry and overwrite check logic
    if overwrite_check & os.path.exists(dem_3dep_dir):
        answer = input('Are you sure you want to overwrite the existing 3DEP DEMs? (yes/y/no/n): ')
        if answer.lower() != 'y' | answer.lower() != 'yes':
            # logger.info('Exiting due to not overwriting.')
            shutil.rmtree(dem_3dep_dir)
        else:
            # logger.info('Exiting due to not overwriting and not retrying.')
            return None
    
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


    # create partial function for 3dep acquisition
    retrieve_process_write_single_3dep_dem_tile_partial = partial(
        retrieve_process_write_single_3dep_dem_tile,
        dem_resolution = dem_resolution,
        crs = crs,
        ndv = ndv,
        dem_3dep_dir = dem_3dep_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext
    )

    # load tile inputs
    tile_inputs = gpd.read_file(tile_inputs_file)

    # download tiles
    dem_tile_file_names = []
    for _, (idx, huc, tile_geom) in tqdm(
        tile_inputs.iterrows(), total=len(tile_inputs), desc="Downloading 3DEP DEMs by tile"
    ):
        fn = retrieve_process_write_single_3dep_dem_tile_partial(idx, huc, tile_geom)
        dem_tile_file_names.append(fn)

    # Submit tasks for processing each tile as soon as its input data is ready
    #processing_futures = []
    #for future in as_completed(input_futures):
    #    tile_inputs = future.result()
    #    for idx, huc, geometry in tile_inputs:
    #        process_future = client.submit(retrieve_process_write_single_3dep_dem_tile_partial, idx, huc, geometry)
    #        processing_futures.append(process_future)
    
    #dem_tile_file_names = []
    #for pf in as_completed(processing_futures):
    #    try:
    #        dem_tile_file_names.append(pf.result())
    #    except:
    #        print(f'Failed to process tile: {pf.result()}')
    #        pass

    breakpoint()

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
        '--dem_resolution',
        '-r',
        type=int,
        required=True,
        help='The DEM resolution in horizontal units of CRS.'
    )

    parser.add_argument(
        '--dem_3dep_dir',
        '-d',
        type=str,
        required=True,
        help='The path to the output 3DEP DEM directory.'
    )

    # PLACEHOLDER: other arguments

    parser.add_argument(
        '--wbd',
        '-w',
        type=str,
        required=False,
        default=WBD_FILE,
        help='The path to the WBD file.'
    )

    # PLACEHOLDER: other arguments

    parser.add_argument(
        '--wbd_layer',
        '-l',
        type=str,
        required=False,
        default=None,
        help='The WBD layer name.'
    )

    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    Acquired_and_process_lidar_dems(**kwargs)