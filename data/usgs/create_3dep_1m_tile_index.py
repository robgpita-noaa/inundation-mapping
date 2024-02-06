#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create 3DEP 1m tile index.

TODO:
    - implement logging

Command Line Usage:
    date_yyymmdd=<YYYYMMDD>; /foss_fim/data/usgs/create_3dep_1m_tile_index.py -t /data/inputs/3dep_dems/lidar_tile_index/usgs_rocky_3dep_1m_tile_index_${date_yyymmdd}.gpkg
"""

from __future__ import annotations
from typing import List
from pyproj import CRS

import subprocess
import argparse
import os
from pathlib import Path
import time
import requests
import tempfile

from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv

srcDir = os.getenv('srcDir')
load_dotenv(os.path.join(srcDir, 'bash_variables.env'))
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

# base url rocky
BASE_URL = 'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/1m/Projects/'

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

def get_tile_urls(folder_urls : List[str]) -> List[str]:
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


def create_3dep_1m_tile_index(
    tile_index_fn : str | Path,
    tile_index_lyr : str | None = None,
    crs : str | CRS = DEFAULT_FIM_PROJECTION_CRS,
    overwrite: bool = False,
    driver: str = 'GPKG',
) -> Path:
    """
    Create 3DEP 1m tile index.
    """

    # handle retry and overwrite check logic
    if os.path.exists(tile_index_fn):
        if overwrite:
            os.remove(tile_index_fn)
        else:
            answer = input('Are you sure you want to overwrite the existing 3DEP 1m DEM tile index (yes/y/no/n): ')
            if (answer.lower() == 'y') | (answer.lower() == 'yes'):
                os.remove(tile_index_fn)
            else:
                print('Exiting ... file already exists and overwrite not confirmed.')
                exit(0)

    # make tile list
    folder_urls = get_folder_urls(BASE_URL)
    tile_urls_list = get_tile_urls(folder_urls)

    # convert crs to string
    if isinstance(crs, CRS):
        crs = crs.to_string()

    # layer name is None
    if tile_index_lyr is None:
        tile_index_lyr = os.path.basename(tile_index_fn).split('.')[0]

    # make directory if it doesn't exist
    tile_index_fn_dir = os.path.dirname(tile_index_fn)
    os.makedirs(tile_index_fn_dir, exist_ok=True)

    # write tempfile with tile list as line-delimited list
    with tempfile.NamedTemporaryFile(mode='w') as f_tmp:

        # write tile list
        for tile_url in tile_urls_list:
            f_tmp.write(f"{tile_url}\n")

        # get file path of tempfile
        f_tmp_path = f_tmp.name

        # subprocess
        subprocess.run(
            [
                'gdaltindex',
                '-f', f'{driver}',
                '-write_absolute_path',
                '-t_srs', crs,
                '-lyr_name', tile_index_lyr,
                str(tile_index_fn),
                '--optfile', f_tmp_path
            ],
            check=True
        )




if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Create 3DEP 1m tile index.')

    parser.add_argument(
        '-t', '--tile-index-fn',
        help='Tile index filename',
        required=True,
        type=str
    )

    parser.add_argument(
        '-l', '--tile-index-lyr',
        help='Tile index layer',
        required=False,
        default=None
    )

    parser.add_argument(
        '-c', '--crs',
        help='Coordinate reference system',
        required=False,
        type=str,
        default=DEFAULT_FIM_PROJECTION_CRS
    )

    parser.add_argument(
        '-o', '--overwrite',
        help='Overwrite existing tile index',
        required=False,
        action='store_true'
    )

    kwargs = vars(parser.parse_args())

    # Create 3DEP 1m tile index.
    create_3dep_1m_tile_index(**kwargs)


