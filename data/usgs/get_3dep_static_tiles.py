#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

Command Line Usage:
    r=<dem_resolution>; /foss_fim/data/usgs/get_3dep_static_tiles.py -r ${r} -t /data/misc/lidar/ngwpc_PI1_lidar_hucs.gpkg -d /data/inputs/3dep_dems/${r}m_5070_lidar_tiles -l ngwpc_PI1_lidar_hucs -o -j 1

Tile Index Command
    DATE=<date>; gdaltindex -f GPKG -write_absolute_path -t_srs EPSG:5070 -lyr_name usgs_rocky_3dep_1m_tile_index usgs_rocky_3dep_1m_tile_index_${DATE}.gpkg --optfile 3dep_file_urls.lst
"""

from __future__ import annotations
from typing import List, Set
from numbers import Number
from pyproj import CRS

import uuid
import shutil
import argparse
import os
from pathlib import Path
from functools import partial
import time
import requests

from bs4 import BeautifulSoup
from shapely.geometry import box
from osgeo import gdal
from rasterio.enums import Resampling
import dask
import numpy as np
import pandas as pd
import geopandas as gpd
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client
from tqdm import tqdm
import rioxarray as rxr


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
WBD_LAYER = f"WBDHU{WBD_SIZE}"

# default CRS
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

# computational and process variables
MAX_RETRIES = 25 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client
DEFAULT_SLEEP_TIME = 0 # default sleep time in seconds for retries

# urls and paths
BASE_URL = "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/1m/Projects/"

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


def retry_request(url : str, max_retries : int = 10, sleep_time : int = 3) -> requests.Response:
    for i in range(max_retries):
        try:
            # TODO: cache with aiohttp
            #async with CachedSession(cache=SQLiteBackend('demo_cache')) as session:
                #await session.get(url)
            response = requests.get(url)
            if response.status_code != 200:
                #print(f"Failed : '{url}'")
                time.sleep(sleep_time)
                continue
            return response
        
        except requests.exceptions.ConnectionError:
            #print(f"Failed : '{url}'")
            time.sleep(sleep_time)
            continue

    if i == (max_retries - 1):
        raise requests.exceptions.ConnectionError(f"Failed to connect to '{url}' after {max_retries} retries")
    elif response.status_code != 200:
        raise requests.exceptions.ConnectionError(
            f"Failed to connect to '{url}' with status code {response.status_code}"
        )
    
def get_folder_urls(base_url : str) -> List[str]:
    """
    Fetch all folder URLs from the base URL
    """
    response = retry_request(base_url)

    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a')

    folder_urls = []
    for link in links:
        href = link.get('href')
        if href and '/' in href and not href.startswith('/'):
            folder_url = base_url + href
            folder_urls.append(folder_url)

    return folder_urls

def get_tile_urls(base_url : str, folder_urls : List[str]) -> List[str]:
    """
    Fetch all .tif file URLs from the TIFF folders in the given folder URLs
    """

    tile_urls = []

    for folder_url in tqdm(folder_urls, desc="Fetching 3DEP tile URLs by project"):
        # Fetch the content of each folder
        response = retry_request(folder_url)

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')

        # Check if TIFF folder exists
        tiff_folder_link = next((link for link in links if 'TIFF' in link.get('href', '')), None)
        if not tiff_folder_link:
            print(f"No TIFF folder found in {folder_url}")
            continue

        # Fetch the content of the TIFF folder
        tiff_folder_url = folder_url + tiff_folder_link.get('href')
        response = retry_request(tiff_folder_url)

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')

        # List all .tif files
        for link in links:
            href = link.get('href')
            if href and href.endswith('.tif'):
                tiff_file_url = '/vsicurl/' + tiff_folder_url + href
                tile_urls.append(tiff_file_url)

    return tile_urls

def get_3dep_tile_list(base_url : str, dest_file: str | Path, overwrite: bool) -> List[str]:
    """
    Fetch all 3DEP 1m DEM tile URLs and write them to a line-delimited text file
    """

    if os.path.exists(dest_file) & (not overwrite):
        with open(dest_file, 'r') as f:
            tile_urls = f.read().splitlines()
        print(f"Read {len(tile_urls)} tile URLs from {dest_file}")
        return tile_urls

    print(f"Fetching tile URLs and writing to {dest_file}")
    folder_urls = get_folder_urls(base_url)
    tile_urls = get_tile_urls(base_url, folder_urls)

    dest_dir = os.path.dirname(dest_file)

    os.makedirs(dest_dir, exist_ok=True)

    # write tile_urls to line-delimited text file
    with open(dest_file, 'w') as f:
        for url in tile_urls:
            f.write(url + '\n')
        print(f"Wrote {len(tile_urls)} tile URLs to {dest_file}")

    return tile_urls

def retrieve_process_write_single_3dep_dem_tile(
    url : str,
    wbd : gpd.GeoDataFrame,
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

    # lazy load dem
    dem = rxr.open_rasterio(url, parse_coordinates=False, mask_and_scale=True)

    # get bounding box and convert to geoseries
    dem_box = (
        gpd.GeoSeries(box(*dem.rio.bounds()), crs=dem.rio.crs)
        .to_crs(wbd.crs)
        .iloc[0]
    )
    
    # check for intersection, return None if no intersection
    if not wbd.intersects(dem_box).any():
        return None
    
    breakpoint()
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
    dem.attrs['TILE_ID'] = str(uuid.uuid4()).replace('-', '')
    dem.attrs['ACQUIRED_DATETIME_UTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # create write path
    # TODO: Verify this logic
    breakpoint()
    project_name = url.split('/')[-3]
    tile_name = url.split('/')[-1].split('.')[0]

    # construct file name
    dem_file_name = os.path.join(dem_tile_dir,f'{project_name}_{tile_name}.{write_ext}')

    # write file
    dem.rio.to_raster(
        dem_file_name,
        **write_kwargs
    )

    # close dem
    dem.close()

    return dem_file_name


def get_3dep_static_tiles(
    dem_resolution : Number,
    dem_3dep_dir : str | Path,
    wbd_file : str | Path | gpd.GeoDataFrame = WBD_FILE,
    wbd_layer : str = 'WBDHU8',
    wbd_buffer : Number = WBD_BUFFER,
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
        #shutil.rmtree(dem_3dep_dir)
        shutil.rmtree(os.path.join(dem_3dep_dir, 'tiles'), ignore_errors=True)
    else:
        answer = input('Are you sure you want to overwrite the existing 3DEP DEMs? (yes/y/no/n): ')
        if (answer.lower() == 'y') | (answer.lower() == 'yes'):
            # logger.info('Exiting due to not overwriting.')
            #shutil.rmtree(dem_3dep_dir)
            shutil.rmtree(os.path.join(dem_3dep_dir, 'tiles'), ignore_errors=True)
        else:
            # logger.info('Exiting due to not overwriting and not retrying.')
            exit()
        
    
    # directory location
    os.makedirs(dem_3dep_dir, exist_ok=True)

    # create dask client
    '''
    if client is None:
        try:
            client = get_client()
        except ValueError:
            client = Client(n_workers=num_workers, threads_per_worker=1)
            local_client = True
        else:
            local_client = False
    '''

    # make tile list
    tile_urls_list_fn = os.path.join(dem_3dep_dir, '3dep_file_urls.lst')
    tile_urls_list = get_3dep_tile_list(base_url = BASE_URL, dest_file = tile_urls_list_fn, overwrite = False)

    # make tile directory
    dem_tile_dir = os.path.join(dem_3dep_dir, 'tiles')
    os.makedirs(dem_tile_dir, exist_ok=True)

    # load wbd
    print(f"Loading WBDs: {wbd_file}, layer: {wbd_layer}...")
    if isinstance(wbd_file, gpd.GeoDataFrame):
        wbd = wbd_file
    else:
        wbd = gpd.read_file(wbd_file, layer=wbd_layer)

    # TEMP: get HUC8 == '12090301'
    wbd = wbd[wbd['HUC8'] == '12090301']

    # buffer wbd
    print(f"Buffering WBDs by {wbd_buffer}...")
    wbd.geometry = wbd.buffer(wbd_buffer)

    # create partial function for 3dep acquisition
    retrieve_process_write_single_3dep_dem_tile_partial = partial(
        retrieve_process_write_single_3dep_dem_tile,
        wbd = wbd,
        dem_resolution = dem_resolution,
        crs = crs,
        ndv = ndv,
        dem_tile_dir = dem_tile_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext
    )
    
    # for debugging
    #tile_urls_list = tile_urls_list[:10]

    #dem_file_name = retrieve_process_write_single_3dep_dem_tile_partial(tile_urls_list[0])
    #breakpoint()

    # download tiles
    #'''
    dem_tile_file_names = [None] * len(tile_urls_list)
    for i, url in tqdm(
        enumerate(tile_urls_list), desc="Downloading 3DEP DEMs by tile", total=len(tile_urls_list)
    ):
        try:
            dem_tile_file_names[i] =  retrieve_process_write_single_3dep_dem_tile_partial(url)
        except Exception as e:
            print(f"Failed to retrieve, process, and write 3DEP DEM tile: {url}")
            pass
    breakpoint()
    #'''
    
    # submit futures
    futures = [client.submit(retrieve_process_write_single_3dep_dem_tile_partial, *row) for row in tile_urls_list]

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
    '''
    if local_client:
        client.close()
    '''
        
    return destVRT


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Acquires and preprocesses 3DEP DEMs for use with HAND FIM.')

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
        '-t', '--wbd-file',
        help='Path to WBD file',
        type=str,
        default=WBD_FILE,
        required=False
    )

    parser.add_argument(
        '-l', '--wbd-layer',
        help='WBD layer',
        type=str,
        default=WBD_LAYER,
        required=False
    )

    parser.add_argument(
        '-b', '--wbd-buffer',
        help='WBD buffer',
        type=Number,
        default=WBD_BUFFER,
        required=False
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

    get_3dep_static_tiles(**kwargs)