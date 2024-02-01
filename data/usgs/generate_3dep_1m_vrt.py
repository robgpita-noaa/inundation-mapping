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
import geopandas as gpd
import rioxarray as rxr
from shapely.geometry import box
from shapely.ops import transform
import pyproj
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

WBD_BUFFER = 5000 # should match buffer size in config file. in CRS horizontal units
WBD_SIZE = 8 # huc size
WBD_FILE = os.path.join(inputsDir, 'wbd', 'WBD_National_EPSG_5070.gpkg') # env file????


DEST_VRT = os.path.join(inputsDir, '3dep_dems', '1m_5070_VRTs', 'tiles_only_3dep_1m_5070.vrt')



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


def request_and_prepare_3dep_tile(tile_urls : str | List, wbd : gpd.GeoDataFrame, dest_dir : str) -> str:

    if isinstance(tile_urls, str):
        with open(os.path.join(os.path.dirname(dest_vrt), '3dep_file_urls.lst'), 'r') as f:
            tile_urls = f.read().splitlines()

    dest_dir_tiles = os.path.join(dest_dir, 'tiles')
    os.makedirs(dest_dir_tiles, exist_ok=True)

    tile_vrts = []
    for tl in tqdm(tile_urls, desc="Reprojecting 3DEP tiles to EPSG:5070"):
        
        basename = os.path.basename(tl)

        dest_tile_vrt = os.path.join(dest_dir_tiles, basename)





def main(dest_vrt: str = DEST_VRT):
    #tile_urls = get_3dep_tile_list(destVRT=dest_vrt)

    wbd = gpd.read_file(WBD_FILE)
        
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
