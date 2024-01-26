#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

Command Line Usage:
    /foss_fim/data/usgs/acquire_and_preprocess_lidar_dems.py -r 30 -d /data/inputs/3dep_dems/10m_test -w /data/misc/lidar/ngwpc_PI1_lidar_hucs.gpkg -l ngwpc_PI1_lidar_hucs
"""

from __future__ import annotations

from typing import List, Set, Tuple, Generator
from numbers import Number

import warnings
import shutil
from glob import glob
from itertools import product
import argparse
import os
from pathlib import Path
from functools import partial
import itertools
import uuid

from osgeo.gdal import BuildVRT, BuildVRTOptions
from rasterio.enums import Resampling
from shapely.geometry import shape, box, Polygon, MultiPolygon
from tqdm.dask import TqdmCallback
from dask.diagnostics import ProgressBar
import dask
import fiona
from pyproj import CRS
import numpy as np
import pandas as pd
import py3dep
from shapely import Polygon, MultiPolygon
import xarray as xr
import rioxarray as rxr
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client


from utils.shared_variables import elev_raster_ndv as ELEV_RASTER_NDV
from shapely.geometry import Polygon, MultiPolygon

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

# TODO: move to shared file
# USGS 3DEP 10m VRT URL
__USGS_3DEP_10M_VRT_URL = (
    r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'
)

# computational and process variables
MAX_PIXELS_3DEP = py3dep.py3dep.MAX_PIXELS # maximum number of pixels available in 3dep service
MAX_RETRIES = 5 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client

WRITE_KWARGS = {
    'driver' : 'COG',
    'dtype' : 'float32',
    'windowed' : True,
    'compute' : True,
    'overwrite' : True,
    'blockxsize' : 256,
    'blockysize' : 256,
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

# disable cache
os.environ["HYRIVER_CACHE_DISABLE"] = "true"

# cache is disabled so these are not used and are PLACEHOLDERS
# path to cache file
os.environ["HYRIVER_CACHE_NAME"] = os.path.join(projectDir, "cache", "hyriver_aiohttp_cache.sqlite")
# time that a cache entry is valid
os.environ["HYRIVER_CACHE_EXPIRE"] = "3600" # seconds


class IdentifierGenerator:
    """
    Generates globally unique identifiers for tiles.
    """
    def __init__(self, counter=1, digits=8):
        self.counter = counter
        self.digits = digits

    def get_next_identifier(self):
        identifier = f"{self.counter:0{self.digits}d}"
        self.counter += 1
        return identifier
    
def tile_geometry(
    geometry: Polygon | MultiPolygon, max_tile_size: Number, tile_buffer: Number
) -> Generator[Polygon | MultiPolygon]:
    """
    Yields box tiles from a given polygon or multi-polygon (un-tested).
    """
    
    # get bounds
    xmin, ymin, xmax, ymax = geometry.bounds

    # get number of tiles
    num_x_tiles = int(np.ceil((xmax - xmin) / max_tile_size))
    num_y_tiles = int(np.ceil((ymax - ymin) / max_tile_size))

    # create fishnet borders without buffer
    tile_borders_x = np.linspace(xmin, xmax, num_x_tiles + 1)
    tile_borders_y = np.linspace(ymin, ymax, num_y_tiles + 1)
    tile_borders = list(product(tile_borders_x, tile_borders_y))

    # update tile_size
    tile_size_x = tile_borders_x[1] - tile_borders_x[0]
    tile_size_y = tile_borders_y[1] - tile_borders_y[0]

    # create tile boxes
    tiles = []
    for x0, y0 in tile_borders:
        
        # Base tile
        tile_xmin, tile_ymin = x0, y0
        tile_xmax = x0 + tile_size_x
        tile_ymax = y0 + tile_size_y

        # Determine if the tile is an edge tile
        is_edge_min_x, is_edge_max_x = tile_xmin == xmin, tile_xmax == xmax
        is_edge_min_y, is_edge_max_y = tile_ymin == ymin, tile_ymax == ymax

        # determine if tile is on left or right edge
        if is_edge_max_x:
            tile_xmin -= tile_buffer
        elif is_edge_min_x:
            tile_xmax += tile_buffer
        else:
            tile_xmin -= tile_buffer
            tile_xmax += tile_buffer
        
        # determine if tile is on top or bottom edge
        if is_edge_max_y:
            tile_ymin -= tile_buffer
        elif is_edge_min_y:
            tile_ymax += tile_buffer
        else:
            tile_ymin -= tile_buffer
            tile_ymax += tile_buffer

        # create buffered tile
        tile = box(tile_xmin, tile_ymin, tile_xmax, tile_ymax)

        # only do it if tile intersects with geometry
        tile = tile.intersection(geometry)

        # check if valid
        tile_not_empty = not tile.is_empty
        tile_is_valid = tile.is_valid
        tile_is_area =  isinstance(tile, Polygon) | isinstance(tile, MultiPolygon)
        
        if tile_not_empty & tile_is_valid & tile_is_area:
            yield tile


def Acquired_and_process_lidar_dems(
    dem_resolution : Number,
    dem_3dep_dir : str | Path,
    write_kwargs : dict = WRITE_KWARGS,
    write_ext : str = 'cog',
    huc_list : List[str] | Set[str] | str | Path | None = None,
    wbd : str | Path | fiona.Collection = WBD_FILE,
    wbd_size : str | int = WBD_SIZE,
    wbd_layer : str | None = None,
    ndv : Number = ELEV_RASTER_NDV,
    num_workers : int = NUM_WORKERS,
    wbd_buffer : Number = WBD_BUFFER,
    pixel_buffer : int = 2,
    retry : bool = False,
    dem_vrt : str | Path = __USGS_3DEP_10M_VRT_URL,
    client : dask.distributed.Client | None = None,
    overwrite_check : bool = False
) -> Path:

    # handle retry and overwrite check logic
    if not retry:
        if overwrite_check & os.path.exists(dem_3dep_dir):
            answer = input('Are you sure you want to overwrite the existing 3DEP DEMs? (yes/y/no/n): ')
            if answer.lower() != 'y' | answer.lower() != 'yes':
                # logger.info('Exiting due to not overwriting.')
                shutil.rmtree(dem_3dep_dir)
            else:
                # logger.info('Exiting due to not overwriting and not retrying.')
                return None
    # PLACEHOLDER: log_file handling
    #elif not os.path.exist(log_file):
    #    raise FileNotFoundError('Retry log file not found.')
    elif not os.path.exist(dem_3dep_dir):
        raise FileNotFoundError('Previous 3DEP DEM directory not found.')
    else:
        # logger.info('Continuing to retry failed 3DEP DEMs.')
        pass
    
    # directory location
    os.makedirs(dem_3dep_dir, exist_ok=True)

    # dask client
    if client is None:
        local_client = True
        client = Client(n_workers=num_workers, threads_per_worker=1)

    # prepare huc_set
    if huc_list is not None:
        try:
            huc_set = pd.read_fwf(
                huc_list, header=None, lineterminator='\n', index_col=False, comment='#'
            )[0].to_list()
        except FileNotFoundError:
            huc_set = {huc_list}
        except ValueError:
            huc_set = set(huc_list)
    else:
        huc_set = None  
    
    # wbd layer
    if wbd_layer is None:
        wbd_layer = f'WBDHU{wbd_size}'
    
    # huc attribute name in wbd
    huc_attribute = f'HUC{wbd_size}'
    
    # load wbd
    if isinstance(wbd, str) | isinstance(wbd, Path):
        wbd = fiona.open(wbd, layer=wbd_layer)
    elif isinstance(wbd, fiona.Collection):
        pass
    else:
        raise TypeError('wbd must be a string, Path, or fiona.Collection')
    
    # get wbd crs: extra logic for fiona collection possibly due to old fiona version
    wbd_crs = CRS(wbd.crs['init']) if isinstance(wbd.crs, dict) & ('init' in wbd.crs) else CRS(wbd.crs)

    # load 3dep vrt
    dem_vrt = rxr.open_rasterio(dem_vrt)
    dem_vrt_crs = CRS(dem_vrt.rio.crs)

    # confirm matching CRS
    if wbd_crs.is_exact_same(dem_vrt_crs):
        raise ValueError('WBD and 3DEP DEM VRT must have the same CRS')

    # given maximum number of pixels available in 3dep service and resolution. Compute tile size in meters
    # TODO: CHECK THIS LOGIC
    tile_buffer = pixel_buffer * dem_resolution # one-sided buffer in horizontal units of CRS
    max_tile_size = np.floor(np.sqrt(MAX_PIXELS_3DEP)) * dem_resolution - 2 * (tile_buffer)

    def generate_inputs_for_3dep_dem_acquisition(
        wbd : fiona.Collection,
        huc_attribute : str,
        huc_set : Set[str],
        wbd_buffer : Number,
        max_tile_size : Number
    ) -> Generator[Tuple[int, str, Polygon | MultiPolygon]]:
        """
        Generates input data for the 3DEP DEM acquisition and processing function.
        """        
        #for row in wbd:

        # get huc number
        #huc = row['properties'][huc_attribute]
        huc = wbd['properties'][huc_attribute]
        
        # skip if not in huc set
        if huc_set is not None:
            if huc not in huc_set:
                #continue
                return
        
        # get geometry
        #geometry = shape(row['geometry'])
        geometry = shape(wbd['geometry'])
        
        # buffer
        geometry = geometry.buffer(wbd_buffer)
        
        # tile geometry
        tiles = tile_geometry(geometry, max_tile_size, tile_buffer)

        # yield input data
        results = []
        for tile in tiles:
            idx = str(uuid.uuid4()).replace('-', '')
            results += [(idx, huc, tile)]

        return results
        

    def retrieve_process_write_single_3dep_dem_tile(
        idx : int,
        huc : str,
        geometry : Polygon | MultiPolygon,
        dem_resolution : Number,
        wbd_crs : str,
        ndv : Number,
        dem_3dep_dir : str,
        write_kwargs : dict,
        write_ext : str,
    ) -> Path:
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
                    dem = py3dep.get_map('DEM', geometry=geometry, resolution=dem_resolution, geo_crs=wbd_crs)
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
                    dem = py3dep.static_3dep_dem(geometry=geometry, crs=wbd_crs, resolution=10)
                    dem_source = "static"
                    acquired_datetime_utc = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    break
                except Exception as e:
                    print(f'Static DEM Failed: {e} - idx: {idx}')

        # reproject and resample
        dem = dem.odc.reproject( 
            wbd_crs,
            resolution=dem_resolution,
            resampling=Resampling.bilinear
        )

        # reset ndv to project value
        # what is the existing ndv? assumes nan
        # TODO: Check static DEM ndv is also nan
        dem.rio.write_nodata(ndv, inplace=True)

        # set attributes
        tile_id = f'{huc}_{idx}'
        dem.attrs['TILE_ID'] = tile_id
        dem.attrs['HUC'] = huc
        dem.attrs['ACQUIRED_DATETIME_UTC'] = acquired_datetime_utc
      
        # create write path
        dem_file_name = Path(os.path.join(dem_3dep_dir,f'{tile_id}.{write_ext}'))

        # write file
        dem = dem.rio.to_raster(
            dem_file_name,
            **write_kwargs
        )

        return dem_file_name

    generate_inputs_for_3dep_dem_acquisition_partial = partial(
        generate_inputs_for_3dep_dem_acquisition,
        huc_attribute = huc_attribute,
        huc_set = huc_set,
        wbd_buffer = wbd_buffer,
        max_tile_size = max_tile_size
    )

    # create partial function for 3dep acquisition
    retrieve_process_write_single_3dep_dem_tile_partial = partial(
        retrieve_process_write_single_3dep_dem_tile,
        dem_resolution = dem_resolution,
        wbd_crs = wbd_crs,
        ndv = ndv,
        dem_3dep_dir = dem_3dep_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext
    )

    # Generate input data for 3DEP acquisition and process them
    input_futures = []
    #with TqdmCallback(desc="Generating inputs", leave=True):
    for w in wbd:
        future_w = client.scatter(w)
        input_futures.append(client.submit(generate_inputs_for_3dep_dem_acquisition_partial, future_w))

    # Submit tasks for processing each tile as soon as its input data is ready
    processing_futures = []
    for future in as_completed(input_futures):
        tile_inputs = future.result()
        for idx, huc, geometry in tile_inputs:
            process_future = client.submit(retrieve_process_write_single_3dep_dem_tile_partial, idx, huc, geometry)
            processing_futures.append(process_future)
    
    dem_tile_file_names = []
    for pf in as_completed(processing_futures):
        try:
            dem_tile_file_names.append(pf.result())
        except:
            print(f'Failed to process tile: {pf.result()}')
            pass

    breakpoint()

    # create vrt
    opts = BuildVRTOptions( 
        xRes=dem_resolution,
        yRes=dem_resolution,
        srcNodata='nan',
        VRTNodata=ndv,
        resampleAlg='bilinear'
    )
    
    destVRT = os.path.join(dem_3dep_dir,f'dem_3dep_{dem_resolution}m.vrt')
    
    if os.path.exists(destVRT):
        os.remove(destVRT)
    
    vrt = BuildVRT(destName=destVRT, srcDSOrSrcDSTab=dem_tile_file_names, options=opts)
    vrt = None

    if local_client:
        # close client
        client.close()
    



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


    """
    def get_tile_from_3DEP_VRT(
            geometry : Polygon | MultiPolygon, dem_vrt : str | xr.DataArray
        ) -> xr.DataArray:
        #Retrieves a tile from the 3DEP VRT.

            # open
            if isinstance(dem_vrt, xr.DataArray):
                pass
            elif isinstance(dem_vrt, str):
                dem_vrt = xr.open_rasterio(dem_vrt)
            
            # clipping
            geometry = gpd.GeoSeries([geometry]).to_json()
            
            try:
                # NOTE: Assumes that the 3DEP VRT is in the same CRS as the geometry????
                dem = dem_vrt.rio.clip(geometry)
            except ValueError:
                print('No tiles available for 3DEP VRT')
                return None
            else:
                return dem
    """