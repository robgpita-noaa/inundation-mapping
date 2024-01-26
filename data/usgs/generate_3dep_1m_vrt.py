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

from bs4 import BeautifulSoup
from tqdm import tqdm
from osgeo import gdal
from dotenv import load_dotenv
# TODO: from aiohttp_client_cache import CachedSession, SQLiteBackend

gdal.UseExceptions()

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

DEST_VRT = os.path.join(inputsDir, '3dep_dems', '1m_5070', '3dep_1m_5070.vrt')

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
        resolution='lowest',
        srcNodata=-999999,
        VRTNodata=-999999,
        resampleAlg='bilinear',
        callback=gdal.TermProgress_nocb,
        VRT_VIRTUAL_OVERVIEWS='yes',
        OverviewList='2 4 8 16 32 64 128 256 512 1024 2048'
    )

    dest_dir = os.path.dirname(destVRT)

    os.makedirs(dest_dir, exist_ok=True)

    if os.path.exists(destVRT):
        os.remove(destVRT)

    print(f"Building VRT: {destVRT}")
    vrt = gdal.BuildVRT(destName=destVRT, srcDSOrSrcDSTab=tile_urls, options=opts)

    return vrt

def reproject_3dep_vrt(srcVRT : str, destVRT : str) -> None:
    """
    Reproject the 3DEP 1m DEM VRT to EPSG:5070
    """
    # command to project vrt
    # gdalwarp -f VRT -t_srs EPSG:5070 -tr 1 1 -r bilinear -overwrite -co "BLOCKXSIZE=128" -co "BLOCKYSIZE=128" 3dep_1m_5070.vrt 3dep_1m_5070_proj.vrt

    # used gdal bindings to project vrt
    src_ds = gdal.Open(destVRT)

    print(f"Reprojecting VRT: {destVRT}")
    dst_ds = gdal.Warp(
        os.path.join(dest_dir, '3dep_1m_5070_proj.vrt'),
        src_ds,
        dstSRS='EPSG:5070',
        xRes=1,
        yRes=1,
        resampleAlg=gdal.GRA_Bilinear,
        options=gdal.WarpOptions(
            format='VRT',
            creationOptions=['BLOCKXSIZE=128', 'BLOCKYSIZE=128'],
        ),
    )

    src_ds = None

def main():
    tile_urls = get_3dep_tile_list()
    vrt = build_3dep_vrt(tile_urls)
    reproject_3dep_vrt(DEST_VRT)


if __name__ == "__main__":

    argparse.ArgumentParser(description="Create 3DEP 1m DEM VRT")
    argparse.add_argument('--dest-vrt', '-d', default=DEST_VRT, help="Destination VRT file")
    
    kwargs = vars(parser.parse_args())
    main(**kwargs)

