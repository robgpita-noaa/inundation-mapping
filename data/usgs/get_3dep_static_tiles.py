#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

TODO:
    - implement logging

Command Line Usage:
    date_yyymmdd=<YYYYMMDD>; /foss_fim/data/usgs/get_3dep_static_tiles.py -t /data/inputs/3dep_dems/lidar_tile_index/usgs_rocky_3dep_1m_tile_index_${date_yyymmdd}.gpkg -d /data/inputs/3dep_dems/${r}m_5070_lidar_tiles -o -j 1
"""

from __future__ import annotations
from typing import List
from numbers import Number
from pyproj import CRS

import uuid
import shutil
import argparse
import os
from pathlib import Path
from functools import partial

from osgeo import gdal
from rasterio.enums import Resampling
import dask
import pandas as pd
import geopandas as gpd
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client
from tqdm import tqdm
import rioxarray as rxr

# Enable exceptions for GDAL
gdal.UseExceptions()

# get directories from env variables
srcDir = os.getenv('srcDir')
inputsDir = os.getenv('inputsDir')

# load env variables
load_dotenv(os.path.join(srcDir, 'bash_variables.env'))

# default CRS
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

# computational and process variables
MAX_RETRIES = 3 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client

# urls and paths
BASE_URL = "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/1m/Projects/"
TEN_M_VRT = os.path.join(inputsDir, '3dep_dems', '10m_5070', 'fim_seamless_3dep_dem_10m_5070.vrt')

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


def retrieve_process_write_single_3dep_dem_tile(
    url : str,
    dem_resolution : Number,
    crs : str | CRS,
    ndv : Number,
    dem_tile_dir : str,
    write_kwargs : dict,
    write_ext : str
) -> str:
    """
    Retrieves and processes a single 3DEP DEM tile.
    """

    # open rasterio dataset
    with rxr.open_rasterio(url, parse_coordinates=False, mask_and_scale=True) as dem:
        
        # reproject, remove nan padding, and set encoded ndv
        dem = (
            dem
            .odc.reproject( 
                crs,
                resolution=dem_resolution,
                resampling=Resampling.bilinear
            )
            .rio.write_nodata(ndv, inplace=True, encoded=True)
        )

        # set attributes
        dem.attrs['TILE_ID'] = str(uuid.uuid4()).replace('-', '')
        dem.attrs['ACQUIRED_DATETIME_UTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        dem.attrs['SOURCE_URL'] = url
        
        # create write path
        url_split = url.split('/')
        project_name = url_split[-3]
        tile_name = url_split[-1].split('.')[0]

        # construct file name
        dem_file_name = os.path.join(dem_tile_dir,f'{project_name}___{tile_name}.{write_ext}')

        # write file
        dem.rio.to_raster(
            dem_file_name,
            **write_kwargs
        )

    return dem_file_name


def get_3dep_static_tiles(
    dem_3dep_dir : str | Path,
    tile_index : str | Path | gpd.GeoDataFrame,
    dem_resolution : Number = 1,
    write_kwargs : dict = WRITE_KWARGS,
    write_ext : str = 'tif',
    overwrite : bool = False,
    crs : str | CRS = DEFAULT_FIM_PROJECTION_CRS,
    ndv : Number = -999999,
    max_retries : int = MAX_RETRIES
) -> str | Path:
    """
    Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

    Parameters
    ----------
    dem_resolution : Number
        DEM resolution in meters.
    dem_3dep_dir : str | Path
        Path to 3DEP DEM directory.
    tile_index : str | Path | gpd.GeoDataFrame
        Path to tile index or GeoDataFrame.
    write_kwargs : dict, default = WRITE_KWARGS
        Write kwargs.
    write_ext : str, default = 'tif'
        Write extension.
    overwrite : bool, default = False
        Force overwrite without prompt.
    crs : str | CRS, default = DEFAULT_FIM_PROJECTION_CRS
        Target desired CRS.
    ndv : Number, default = -999999
        No data value.
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
    if overwrite:
        #shutil.rmtree(dem_3dep_dir)
        shutil.rmtree(os.path.join(dem_3dep_dir), ignore_errors=True)
    else:
        answer = input('Are you sure you want to overwrite the existing 3DEP DEMs? (yes/y/no/n): ')
        if (answer.lower() == 'y') | (answer.lower() == 'yes'):
            # logger.info('Exiting due to not overwriting.')
            #shutil.rmtree(dem_3dep_dir)
            shutil.rmtree(os.path.join(dem_3dep_dir), ignore_errors=True)
        else:
            # logger.info('Exiting due to not overwriting and not retrying.')
            exit()
        
    
    # directory location
    os.makedirs(dem_3dep_dir, exist_ok=True)
    dem_tile_dir = os.path.join(dem_3dep_dir, 'tiles')
    os.makedirs(dem_tile_dir, exist_ok=True)

    # load tiles
    if isinstance(tile_index, (str, Path)):
        tile_index = gpd.read_file(tile_index)
    
    # get tile urls
    tile_urls_list = tile_index['location'].tolist()

    # create partial function for 3dep acquisition
    retrieve_process_write_single_3dep_dem_tile_partial = partial(
        retrieve_process_write_single_3dep_dem_tile,
        dem_resolution = dem_resolution,
        crs = crs,
        ndv = ndv,
        dem_tile_dir = dem_tile_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext
    )
    
    # for debugging
    '''
    breakpoint()
    dem_file_name = retrieve_process_write_single_3dep_dem_tile_partial(tile_urls_list[0])
    breakpoint()

    # download tiles serially
    #tile_urls_list = tile_urls_list[:10]
    dem_tile_file_names = [None] * len(tile_urls_list)
    for i, url in tqdm(
        enumerate(tile_urls_list), desc="Downloading 3DEP DEMs by tile", total=len(tile_urls_list)
    ):
        try:
            dem_tile_file_names[i] =  retrieve_process_write_single_3dep_dem_tile_partial(url)
        except Exception as e:
            print(f"Failed to retrieve, process, and write 3DEP DEM tile: {url}")
            pass
    
    # remove None values
    dem_tile_file_names = [x for x in dem_tile_file_names if x is not None]
    breakpoint()
    '''

    # get dask client
    client = get_client()

    # debug
    #tile_urls_list = tile_urls_list[:10]

    # submit futures
    futures = [client.submit(retrieve_process_write_single_3dep_dem_tile_partial, url) for url in tile_urls_list]

    # Dictionary to keep track of retries
    retries = {future: 0 for future in futures}

    # create pbar
    pbar = tqdm(total=len(futures), desc=f"Downloading 3DEP DEMs at {dem_resolution}m")

    # Loop through the futures, checking for exceptions and resubmitting the task if necessary
    dem_tile_file_names = [None] * len(futures)
    for future in as_completed(futures):
        # Get the index of the future
        idx = futures.index(future)
        
        try:
            dem_tile_file_names[idx] = future.result()  # You can use the result here if needed
        except Exception as e:

            # Find the original arguments used for the failed future
            url = tile_urls_list[idx]
            
            if retries[future] < max_retries:
                # Increment the retry count for this future
                retries[future] += 1
                
                # Resubmit the task directly using client.submit
                new_future = client.submit(retrieve_process_write_single_3dep_dem_tile_partial, url)
                
                # Replace the failed future with the new future in the list and update the retries dictionary
                futures[idx] = new_future
                retries[new_future] = retries[future]
            
            else:
                # If the maximum number of retries has been reached, print an error message
                print(f"Failed to retrieve, process, and write 3DEP DEM tile: {url}")

        finally:
            pbar.update(1)

    # close pbar
    pbar.close()

    # remove None values
    dem_tile_file_names = [f for f in dem_tile_file_names if f is not None]

    return dem_tile_file_names


def create_3dep_dem_vrts(
    dem_tile_file_names : List[str | Path],
    dem_resolution : Number,
    dem_3dep_dir : str | Path,
    ndv : Number,
    ten_m_vrt : str | Path
) -> str | Path:
    """
    Creates seamless 3DEP DEM VRTs.
    """

    # create vrt
    opts = gdal.BuildVRTOptions( 
        xRes=dem_resolution,
        yRes=dem_resolution,
        srcNodata=ndv,
        VRTNodata=ndv,
        resampleAlg='bilinear',
        callback=gdal.TermProgress_nocb
    )

    # mosaic with 10m VRT
    seamless_vrt_fn = os.path.join(dem_3dep_dir, f'fim_seamless_3dep_dem_{dem_resolution}m_5070.vrt')

    # create source file list with tiles first and 10m vrt last
    src_files = dem_tile_file_names + [ten_m_vrt]

    if os.path.exists(seamless_vrt_fn):
        os.remove(seamless_vrt_fn)

    print(f"Mosaic Tile VRT with 10m VRT: {seamless_vrt_fn}")
    vrt = gdal.BuildVRT(
        destName=seamless_vrt_fn,
        srcDSOrSrcDSTab=src_files,
        options=opts
    )
    vrt = None

    # build image overviews
    #print(f"Building Image Overviews: {seamless_vrt_fn}")
    # may not need to reopen
    #vrt = gdal.Open(seamless_vrt_fn, gdal.GA_Update) # or gdal.GA_ReadOnly

    # set CPUs for overview
    #gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
    #gdal.SetConfigOption('NUM_THREADS', 'ALL_CPUS')

    # build overviews
    #vrt.BuildOverviews('AVERAGE', [2, 4, 8, 16, 32, 64, 128, 256, 512], gdal.TermProgress_nocb)
    #vrt = None
        
    return seamless_vrt_fn


def main(kwargs):
    """
    Main function for acquiring and preprocessing 3DEP DEMs for use with HAND FIM.
    """

    # pop kwargs
    num_workers = kwargs.pop('num_workers')
    ten_m_vrt = kwargs.pop('ten_m_vrt')

    # acquire and preprocess 3dep dems
    with Client(n_workers=num_workers, threads_per_worker=1) as client:
        dem_tile_file_names = get_3dep_static_tiles(**kwargs)

    # create vrt
    kwargs = { k : kwargs[k] for k in ['dem_3dep_dir','dem_resolution','ndv']}
    kwargs['ten_m_vrt'] = ten_m_vrt

    seamless_vrt_fn = create_3dep_dem_vrts(dem_tile_file_names, **kwargs)

    return seamless_vrt_fn


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Acquires and preprocesses 3DEP DEMs for use with HAND FIM.')

    parser.add_argument(
        '-d', '--dem-3dep-dir',
        help='Path to 3DEP DEM directory',
        type=str,
        required=True
    )

    parser.add_argument(
        '-t', '--tile-index',
        help='Path to tile index',
        type=str,
        required=True
    )

    parser.add_argument(
        '-r', '--dem-resolution',
        help='DEM resolution in meters',
        type=Number,
        required=False,
        default=1
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
        '-o', '--overwrite',
        help='Force overwrite without prompt',
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
        default=-999999,
        required=False
    )

    parser.add_argument(
        '-v', '--ten-m-vrt',
        help='Path to 10m VRT',
        type=str,
        default=TEN_M_VRT,
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
        '-m', '--max-retries',
        help='Max retries',
        type=int,
        default=MAX_RETRIES,
        required=False
    )
    
    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    # Run main function.
    seamless_vrt_fn = main(kwargs)