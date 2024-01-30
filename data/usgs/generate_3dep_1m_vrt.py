#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create 3DEP 1m DEM VRT
"""
from __future__ import annotations

from typing import List

import requests
import argparse
import time
import os
import logging
import datetime

from bs4 import BeautifulSoup
from tqdm import tqdm
from osgeo import gdal
from dotenv import load_dotenv
# TODO: from aiohttp_client_cache import CachedSession, SQLiteBackend

logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(
    '/data/inputs/3dep_dems/gdal_log_{:%Y-%m-%d_%H:%M:%S}.log'.format(datetime.datetime.now())
)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

gdal.UseExceptions()
gdal.ConfigurePythonLogging(logger_name=logger.name)

# aiohttp backend
"""
cache = SQLiteBackend(
    cache_name='~/.cache/aiohttp-requests.db',  # For SQLite, this will be used as the filename
    expire_after=60*60,                         # By default, cached responses expire in an hour
    urls_expire_after={'*.fillmurray.com': -1}, # Requests for any subdomain on this site will never expire
    allowed_codes=(200, 418),                   # Cache responses with these status codes
    allowed_methods=['GET', 'POST'],            # Cache requests with these HTTP methods
    include_headers=True,                       # Cache requests with different headers separately
    ignored_params=['auth_token'],              # Keep using the cached response even if this param changes
    timeout=2.5,                                # Connection timeout for SQLite backend
)
"""

BASE_URL = "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/1m/Projects/"

inputsDir = os.getenv('inputsDir')
srcDir = os.getenv('srcDir')
load_dotenv(os.path.join(srcDir, 'bash_variables.env'))

DEST_VRT = os.path.join(inputsDir, '3dep_dems', '1m_5070_VRTs', 'tiles_only_3dep_1m_5070.vrt')

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

def get_3dep_tile_list(base_url : str = BASE_URL, destVRT : str = DEST_VRT) -> List[str]:
    """
    Fetch all 3DEP 1m DEM tile URLs and write them to a line-delimited text file
    """

    folder_urls = get_folder_urls(base_url)
    tile_urls = get_tile_urls(base_url, folder_urls)

    dest_dir = os.path.dirname(destVRT)

    os.makedirs(dest_dir, exist_ok=True)

    # write tile_urls to line-delimited text file
    tile_urls_file = os.path.join(dest_dir, '3dep_file_urls.lst')
    with open(tile_urls_file, 'w') as f:
        for url in tile_urls:
            f.write(url + '\n')
        print(f"Wrote {len(tile_urls)} tile URLs to {tile_urls_file}")

    return tile_urls

def build_3dep_vrt(tile_urls : List[str], destVRT : str = DEST_VRT) -> gdal.Dataset:
    """
    Build a VRT from the list of 3DEP 1m DEM tile URLs
    """

    opts = gdal.BuildVRTOptions( 
        xRes=1,
        yRes=1,
        srcNodata=-999999,
        VRTNodata=-999999,
        resampleAlg='bilinear',
        callback=gdal.TermProgress_nocb
    )

    dest_dir = os.path.dirname(destVRT)

    os.makedirs(dest_dir, exist_ok=True)

    if os.path.exists(destVRT):
        os.remove(destVRT)

    print(f"Building VRT: {destVRT}")
    vrt = gdal.BuildVRT(destName=destVRT, srcDSOrSrcDSTab=tile_urls, options=opts)

    logger.info("VRT file created successfully.")

    return vrt

def reproject_3dep_vrt(src_file : str, destVRT : str) -> None:
    """
    Reproject the 3DEP 1m DEM VRT to EPSG:5070
    """
    # command to project vrt
    # gdalwarp -f VRT -t_srs EPSG:5070 -tr 1 1 -r bilinear -overwrite -co "BLOCKXSIZE=128" -co "BLOCKYSIZE=128" 3dep_1m_5070.vrt 3dep_1m_5070_proj.vrt

    # used gdal bindings to project vrt
    src_ds = gdal.Open(src_file)

    # remove existing vrt
    if os.path.exists(destVRT):
        os.remove(destVRT)

    opts = gdal.WarpOptions(
        format='VRT',
        creationOptions=['BLOCKXSIZE=128', 'BLOCKYSIZE=128'],
        dstSRS='EPSG:5070',
        xRes=1,
        yRes=1,
        resampleAlg=gdal.GRA_Bilinear,
        #callback=gdal.TermProgress_nocb,
        multithread="True",
        outputType=gdal.GDT_Float32,
        dstNodata=-999999,
        warpOptions=['NUM_THREADS=ALL_CPUS']
    )

    dst_ds = gdal.Warp(
        destVRT,
        src_ds,
        options=opts,
    )

    src_ds = None

def main(dest_vrt: str = DEST_VRT):
    #tile_urls = get_3dep_tile_list(destVRT=dest_vrt)

    with open(os.path.join(os.path.dirname(dest_vrt), '3dep_file_urls.lst'), 'r') as f:
        tile_urls = f.read().splitlines()
    
    tile_vrts = []
    for tl in tqdm(tile_urls, desc="Reprojecting 3DEP tiles to EPSG:5070"):
        
        basename = os.path.basename(tl)
        dest_dir = os.path.dirname(dest_vrt)
        dest_dir_tiles = os.path.join(dest_dir, 'tiles')
        
        os.makedirs(dest_dir_tiles, exist_ok=True)

        dest_tile_vrt = os.path.join(dest_dir_tiles, basename)
        
        dest_tile_vrt = os.path.join(dest_dir_tiles, os.path.splitext(dest_tile_vrt)[0] + '.vrt')
        
        retries = 1
        success = False
        while retries <= 5:
            try:
                #reproject_3dep_vrt(tl, dest_tile_vrt)
                pass
            except Exception as e:
                logger.error(f"Failed to reproject {tl} to EPSG:5070. Retrying...")
                retries += 1
                continue
            else:
                success = True 
                tile_vrts.append(dest_tile_vrt)
                break

        if not success:
            logger.error(f"Failed to reproject {tl} to EPSG:5070 after {retries} retries. Skipping...")
            print(f"Failed to reproject {tl} to EPSG:5070 after {retries} retries. Skipping...")

    breakpoint()
    # write tile_vrts to line-delimited text file
    tile_vrts_file = os.path.join(dest_dir, '3dep_tile_vrts.lst')
    with open(tile_vrts_file, 'w') as f:
        for url in tqdm(tile_vrts, desc="Writing tile VRT list to file"):
            f.write(url + '\n')
        print(f"Wrote {len(tile_vrts)} tile VRTs to {tile_vrts_file}")

    breakpoint()


    # build 3dep_1m_5070.vrt from tile_vrts
    # gdalbuildvrt -tr 1 1 -r bilinear -input_file_list 3dep_tile_vrts.lst -srcnodata -999999 -vrtnodata -999999 tiles_only_3dep_1m_5070.vrt

    # reproject USGS 10m VRT file to EPSG:5070
    # te represents the bounding box of the 1m DEM tiles from the tiles_only_3dep_1m_5070.vrt
    #bounds="-6113670.919 86109.555 3414922.081 3170055.445"
    # gdalwarp -overwrite -of VRT -t_srs EPSG:5070 -tr 1 1 -r bilinear -te_srs EPSG:5070 -te $bounds -co BLOCKYSIZE=128 -co BLOCKXSIZE=128 /vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt seamless_usgs_dem_5070_1m.vrt

    # build final VRT file with 3dep_1m_5070.vrt and USGS_Seamless_DEM_13_proj.vrt
    # gdalbuildvrt -tr 1 1 -r bilinear -overwrite 3dep_1m_5070.vrt tiles_only_3dep_1m_5070.vrt seamless_usgs_dem_5070_1m.vrt


    #vrt = build_3dep_vrt(tile_vrts, dest_vrt)
    


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create 3DEP 1m DEM VRT")
    parser.add_argument('--dest-vrt', '-d', default=DEST_VRT, help="Destination VRT file")
    
    kwargs = vars(parser.parse_args())
    main(**kwargs)

    # heterogeneous tiles
    # AR_North_Corridor_B3_2017/
    # AR_North_Corridor_B2_2017
    # AR_North_Corridor_B1_2017
    # AR_NRCS_Languille_2011
    # AR_NRCS_Cache_2011
