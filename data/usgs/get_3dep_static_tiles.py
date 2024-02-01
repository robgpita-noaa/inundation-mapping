#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

Command Line Usage:
    r=<dem_resolution> /foss_fim/data/usgs/lidar_dems_from_service.py -r ${r} -t /data/misc/lidar/ngwpc_PI1_lidar_tiles_${r}m.gpkg -d /data/inputs/3dep_dems/${r}m_5070_py3dep -o
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
from shapely import Polygon, MultiPolygon
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client
from tqdm import tqdm
import rioxarray as rxr
import pyproj
from shapely.ops import transform
import requests
from bs4 import BeautifulSoup


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
MAX_RETRIES = 10 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client
DEFAULT_SLEEP_TIME = 10 # default sleep time in seconds for retries

# 3DEP variables
BASE_URL = "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/1m/Projects/"

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

def retry_request(url : str, max_retries : int = 5, sleep_time : int = 3) -> requests.Response:
    for i in range(max_retries):
        try:
            # TODO: cache with aiohttp
            #async with CachedSession(cache=SQLiteBackend('demo_cache')) as session:
                #await session.get(url)
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Failed : '{url}'")
                time.sleep(sleep_time)
                continue
            return response
        
        except requests.exceptions.ConnectionError:
            print(f"Failed : '{url}'")
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

def get_3dep_tile_list(base_url : str = BASE_URL, dest_file: str | Path) -> List[str]:
    """
    Fetch all 3DEP 1m DEM tile URLs and write them to a line-delimited text file
    """

    folder_urls = get_folder_urls(base_url)
    tile_urls = get_tile_urls(base_url, folder_urls)

    # write tile_urls to line-delimited text file
    with open(dest_file, 'w') as f:
        for url in tile_urls:
            f.write(url + '\n')
        print(f"Wrote {len(tile_urls)} tile URLs to {dest_file}")

    return tile_urls


def bounds_intersect(rioxarray_data, geodataframe, target_crs):
    # Step 1: Extract bounds from the rioxarray object
    minx, miny, maxx, maxy = rioxarray_data.rio.bounds()

    # Step 2: Create a box from these bounds
    bounds_box = box(minx, miny, maxx, maxy)

    # Initialize the pyproj transformer for CRS transformation
    # Source CRS from the rioxarray object
    source_crs = pyproj.CRS(rioxarray_data.rio.crs)
    # Target CRS from the GeoDataFrame
    #target_crs = pyproj.CRS(geodataframe.crs)
    transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

    # Step 3: Transform the box geometry to the target CRS
    transformed_box = transform(transformer.transform, bounds_box)

    # Step 4: Check for intersection with the GeoDataFrame geometries
    return geodataframe.intersects(transformed_box)


def request_and_prepare_3dep_tile(
    tile_urls : str | List,
    wbd : gpd.GeoDataFrame,
    target_crs : str | CRS,
    dem_resolution : Number,
    dest_dir : str,
    write_kwargs : dict,
    ndv : Number
) -> str:


    dem = rxr.open_rasterio(tl)

    # check for intersection with wbd
    breakpoint()
    intersects = bounds_intersect(dem, wbd, target_crs)

    if intersects:

        # reproject and resample
        dem = dem.odc.reproject( 
            target_crs,
            resolution=dem_resolution,
            resampling=Resampling.bilinear
        )

        # reset ndv to project value
        # existing ndv isclose to -3.4028234663852886e+38
        existing_ndv = -3.4028234663852886e+38
        dem.values[np.isclose(dem, existing_ndv)] = ndv

        dem.rio.write_nodata(ndv, inplace=True)

        # set attributes
        dem.attrs['ACQUIRED_DATETIME_UTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        basename = os.path.basename(tl)

        dest_tile_fn = os.path.join(dest_dir_tiles, basename)

        # write file
        dem = dem.rio.to_raster(
            dest_tile_fn,
            **write_kwargs
        )



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
    sleep_time : int
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
            retries += 1
            print(f'Dynamic DEM Failed: {e} - idx: {idx} | retries: {retries}')

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
                print(f'Retrying in {sleep_time} seconds - idx: {idx}')
                time.sleep(sleep_time)
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
    num_workers : int = NUM_WORKERS,
    sleep_time : int = DEFAULT_SLEEP_TIME
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
    sleep_time : int, default = DEFAULT_SLEEP_TIME
        Sleep time in seconds for retries.

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

    # make tile directory
    dem_tile_dir = os.path.join(dem_3dep_dir, 'tiles')
    os.makedirs(dem_tile_dir, exist_ok=True)

    # create partial function for 3dep acquisition
    request_and_prepare_3dep_tile_partial = partial(
        request_and_prepare_3dep_tile,
        tile_urls=tile_urls,
        wbd=wbd,
        target_crs=crs,
        dem_resolution=dem_resolution,
        dest_dir=dem_tile_dir,
        write_kwargs=write_kwargs,
        ndv=ndv
    )

    dest_tile_urls_fn = os.path.join(dest_dir, '3dep_file_urls.lst')

    #tile_urls = get_3dep_tile_list(base_url=BASE_URL, dest_tile_urls_fn)
    
    if isinstance(tile_urls, str):
        with open(os.path.join(os.path.dirname(dest_vrt), '3dep_file_urls.lst'), 'r') as f:
            tile_urls = f.read().splitlines()
    
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

    parser.add_argument(
        '-s', '--sleep-time',
        help='Sleep time in seconds for retries',
        type=int,
        default=DEFAULT_SLEEP_TIME,
        required=False
    )
    
    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    acquired_and_process_lidar_dems(**kwargs)