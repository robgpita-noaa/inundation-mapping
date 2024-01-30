#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tiles WBDs.

Command Line Usage:
    /foss_fim/data/usgs/tile_wbds.py -r 10 --write_path /data/misc/lidar/ngwpc_PI1_lidar_tiles.gpkg --wbd /data/misc/lidar/ngwpc_PI1_lidar_hucs.gpkg -l ngwpc_PI1_lidar_hucs
"""
from __future__ import annotations
from typing import List, Set, Tuple, Generator
from numbers import Number

import shutil
from itertools import product
import argparse
import os
from pathlib import Path
from functools import partial
import itertools
import uuid
import json

from shapely.geometry import shape, box, Polygon, MultiPolygon
from tqdm.dask import TqdmCallback
import fiona
from pyproj import CRS
import numpy as np
import geopandas as gpd
import py3dep
from dotenv import load_dotenv
from dask.distributed import Client, get_client, as_completed
from tqdm import tqdm


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
MAX_PIXELS_3DEP = py3dep.py3dep.MAX_PIXELS # maximum number of pixels available in 3dep service
MAX_RETRIES = 5 # number of retries for 3dep acquisition
NUM_WORKERS = os.cpu_count() - 1 # number of workers for dask client

WRITE_KWARGS = {'index' : False}

    
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

def generate_inputs_for_3dep_dem_acquisition(
    wbd : fiona.Collection,
    huc_attribute : str,
    wbd_buffer : Number,
    max_tile_size : Number,
    tile_buffer : Number
) -> List[Tuple[int, str, Polygon | MultiPolygon]]:
    """
    Generates input data for the 3DEP DEM acquisition and processing function.
    """        
    #for row in wbd:

    # get huc number
    #huc = row['properties'][huc_attribute]
    huc = wbd['properties'][huc_attribute]
    
    # get geometry
    #geometry = shape(row['geometry'])
    geometry = shape(wbd['geometry'])
    
    # buffer
    geometry = geometry.buffer(wbd_buffer)
    
    # tile geometry
    tiles = tile_geometry(geometry, max_tile_size, tile_buffer)

    return [(str(uuid.uuid4()).replace('-', ''), huc, tile) for tile in tiles]


def tile_wbds(
    dem_resolution : Number,
    wbd : str | Path | fiona.Collection = WBD_FILE,
    wbd_size : str | int = WBD_SIZE,
    wbd_layer : str | None = None,
    wbd_buffer : Number = WBD_BUFFER,
    pixel_buffer : int = 2,
    write_path : str | Path | None = None,
    write_kwargs : dict = WRITE_KWARGS,
    client : Client | None = None,
    num_workers : int = NUM_WORKERS,
    overwrite_check : bool = False
) -> gpd.GeoDataFrame:

    # handle retry and overwrite check logic
    if overwrite_check & (write_path is not None):
        if os.path.exists(write_path):
            answer = input('Are you sure you want to overwrite the existing 3DEP DEM tiles? (yes/y/no/n): ')
            if answer.lower() != 'y' | answer.lower() != 'yes':
                shutil.rmtree(write_path)
            else:
                return None

    # get dask client
    if client is None:
        try:
            client = get_client()
        except ValueError:
            client = Client(n_workers=num_workers, threads_per_worker=1)
            local_client = True
        else:
            local_client = False
    
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

    # given maximum number of pixels available in 3dep service and resolution. Compute tile size in meters
    # TODO: CHECK THIS LOGIC
    tile_buffer = pixel_buffer * dem_resolution # one-sided buffer in horizontal units of CRS
    max_tile_size = np.floor(np.sqrt(MAX_PIXELS_3DEP)) * dem_resolution - 2 * (tile_buffer)
        

    generate_inputs_for_3dep_dem_acquisition_partial = partial(
        generate_inputs_for_3dep_dem_acquisition,
        huc_attribute = huc_attribute,
        wbd_buffer = wbd_buffer,
        max_tile_size = max_tile_size,
        tile_buffer = tile_buffer
    )

    # generate inputs and download tiles in a for loop
    # create dem_tile_file_names list
    results = [generate_inputs_for_3dep_dem_acquisition_partial(w) for w in wbd]
    results = list(itertools.chain.from_iterable(results))

    # PLACEHOLDER
    if True:
        # Generate input data for 3DEP acquisition and process them
        futures = []
        for w in wbd:
            future_w = client.scatter(w)
            futures.append(client.submit(generate_inputs_for_3dep_dem_acquisition_partial, future_w))

        results = []
        with tqdm(total=len(futures), desc="Downloading 3DEP DEMs by tile") as pbar:
            for f in as_completed(futures):
                results.append(f.result())
                pbar.update(1)
    
        # Get results
        results = client.gather(futures)
        #breakpoint()

    # generate geodataframe
    results = gpd.GeoDataFrame(
        itertools.chain.from_iterable(results),
        columns=['idx', 'huc', 'geometry'],
        crs=wbd_crs
    )

    # write to file
    if write_path is not None:
        
        if os.path.exists(write_path):
            os.remove(write_path)

        if write_kwargs is None:
            write_kwargs = {}
        
        # write to file
        results.to_file(write_path, **write_kwargs)

    # close client
    if local_client:
        client.close()

    return results


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Tiles WBDs.')


    parser.add_argument(
        '--dem_resolution',
        '-r',
        type=int,
        required=True,
        help='The DEM resolution in horizontal units of CRS.'
    )

    parser.add_argument(
        '--wbd',
        '-b',
        type=str,
        required=False,
        default=WBD_FILE,
        help='The path to the WBD file.'
    )

    parser.add_argument(
        '--wbd_size',
        '-s',
        type=int,
        required=False,
        default=WBD_SIZE,
        help='The size of the WBD.'
    )

    parser.add_argument(
        '--wbd_layer',
        '-l',
        type=str,
        required=False,
        default=None,
        help='The layer of the WBD.'
    )

    parser.add_argument(
        '--wbd_buffer',
        '-f',
        type=int,
        required=False,
        default=WBD_BUFFER,
        help='The buffer size of the WBD.'
    )

    parser.add_argument(
        '--pixel_buffer',
        '-p',
        type=int,
        required=False,
        default=2,
        help='The buffer size in pixels.'
    )

    parser.add_argument(
        '--write_path',
        '-w',
        type=str,
        required=False,
        help='The path to write the data to.'
    )

    parser.add_argument(
        '--write_kwargs',
        '-k',
        type=json.loads,
        required=False,
        default=WRITE_KWARGS,
        help='The write kwargs.'
    )

    parser.add_argument(
        '--num_workers',
        '-n',
        type=int,
        required=False,
        default=NUM_WORKERS,
        help='The number of workers.'
    )

    parser.add_argument(
        '--overwrite_check',
        '-o',
        action='store_true',
        required=False,
        help='The overwrite check.'
    )
    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    tile_wbds(**kwargs)