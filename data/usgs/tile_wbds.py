#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tiles WBDs.

Command Line Usage:
    r=<dem_resolution>; /foss_fim/data/usgs/tile_wbds.py -r ${r} --write_path /data/misc/lidar/ngwpc_PI1_lidar_tiles_${r}m.gpkg --wbd /data/misc/lidar/ngwpc_PI1_lidar_hucs.gpkg -l ngwpc_PI1_lidar_hucs
"""
from __future__ import annotations
from typing import Generator
from numbers import Number

from itertools import product
import argparse
import os
from pathlib import Path
import uuid
import json

from shapely.geometry import box, Polygon, MultiPolygon
import numpy as np
import geopandas as gpd
import py3dep
from dotenv import load_dotenv


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

        tile_xmin -= tile_buffer
        tile_ymin -= tile_buffer
        tile_xmax += tile_buffer
        tile_ymax += tile_buffer

        # create buffered tile
        tile = box(tile_xmin, tile_ymin, tile_xmax, tile_ymax)

        # buffer tile
        #tile = tile.buffer(tile_buffer)

        # checks
        tile_intersects = tile.intersects(geometry)
        tile_not_empty = not tile.is_empty
        tile_is_valid = tile.is_valid
        tile_is_area =  isinstance(tile, Polygon) | isinstance(tile, MultiPolygon)
        
        if tile_intersects & tile_not_empty & tile_is_valid & tile_is_area:
            yield tile


def tile_wbds(
    dem_resolution : Number,
    wbd : str | Path | gpd.GeoDataFrame = WBD_FILE,
    wbd_size : str | int = WBD_SIZE,
    wbd_layer : str | None = None,
    wbd_buffer : Number = WBD_BUFFER,
    pixel_buffer : int = 100,
    write_path : str | Path | None = None,
    write_kwargs : dict = WRITE_KWARGS,
    waive_overwrite_check : bool = False
) -> gpd.GeoDataFrame:
    """
    Tiles WBDs in preparation for 3DEP DEM acquisition. Writes the tiled WBDs to a file if write_path is not None.

    Parameters
    ----------
    dem_resolution : Number
        The DEM resolution in horizontal units of CRS.
    wbd : str | Path | gpd.GeoDataFrame, default = WBD_FILE
        The path to the WBD file or the WBD file itself.
    wbd_size : str | int, default = WBD_SIZE
        The size of the WBD. Options are 2, 4, 6, 8, 10, 12.
    wbd_layer : str | None, default = None
        The layer of the WBD.
    wbd_buffer : Number, default = WBD_BUFFER
        The buffer size of the WBD in CRS horizontal units.
    pixel_buffer : int, default = 100
        The buffer size in pixels.
    write_path : str | Path | None, default = None
        The path to write the data to.
    write_kwargs : dict, default = WRITE_KWARGS
        The write kwargs.
    waive_overwrite_check : bool, default = False
        Waive overwrite check user input.

    Returns
    -------
    gpd.GeoDataFrame
        The tiled WBDs.

    Raises
    ------
    TypeError
        If wbd is not a string, Path, or gpd.GeoDataFrame.
    """

    # handle retry and overwrite check logic
    if (not waive_overwrite_check) & (write_path is not None):
        if os.path.exists(write_path):
            answer = input('Are you sure you want to overwrite the existing 3DEP DEM tiles? (yes/y/no/n): ')
            if (answer.lower() == 'y') | (answer.lower() == 'yes'):
                os.remove(write_path)
            else:
                exit()
    elif write_path is not None:
        if os.path.exists(write_path):
            os.remove(write_path)
    
    # wbd layer
    if wbd_layer is None:
        wbd_layer = f'WBDHU{wbd_size}'
    
    # huc attribute name in wbd
    huc_attribute = f'HUC{wbd_size}'
    
    # load wbd
    print(f'Loading WBDs: {wbd}, layer: {wbd_layer}...')
    wbd_gdf = gpd.read_file(wbd, layer=wbd_layer)

    # check if wbd is valid
    print('Checking if WBDs are valid...')
    if wbd_gdf.is_valid.all() == False:
        wbd_gdf.geometry = wbd_gdf.make_valid()

    # buffer wbd
    print(f'Buffering WBDs by {wbd_buffer}...')
    wbd_buffered = wbd_gdf.copy()
    wbd_buffered.geometry = wbd_gdf.buffer(wbd_buffer)

    # unary union
    print('Creating unary union of WBDs...')
    wbd_unary_union_shape = wbd_buffered.unary_union

    # given maximum number of pixels available in 3dep service and resolution. Compute tile size in meters
    # TODO: CHECK THIS LOGIC
    tile_buffer = pixel_buffer * dem_resolution # one-sided buffer in horizontal units of CRS
    max_tile_size = np.floor(np.sqrt(MAX_PIXELS_3DEP)) * dem_resolution - 2 * (tile_buffer)

    # tiles from wbd
    print('Creating tiles from WBDs...')
    tiles = tile_geometry(wbd_unary_union_shape, max_tile_size, tile_buffer)

    # generate geodataframe
    tiles_gdf = gpd.GeoDataFrame(
        tiles,
        columns = ['geometry'],
        crs = wbd_gdf.crs
    )

    # makes tile ids
    tiles_gdf['tile_id'] = [str(uuid.uuid4()).replace('-', '') for _ in range(len(tiles_gdf))]

    # tile centroids
    tiles_centroids = tiles_gdf.copy()
    tiles_centroids['geometry'] = tiles_gdf.centroid

    # spatial join
    print('Finding the nearest WBD to every tile centroid...')
    tile_ids_by_huc = gpd.sjoin_nearest(tiles_centroids, wbd_gdf, how='left')[[huc_attribute, 'tile_id']]

    # merge hucs to tiles
    tiles_gdf = tiles_gdf.merge(tile_ids_by_huc, on='tile_id')
    tiles_gdf = tiles_gdf[['tile_id', huc_attribute, 'geometry']]

    # write to file
    if write_path is not None:
        print(f'Writing tiles to {write_path}...')
        
        if os.path.exists(write_path):
            os.remove(write_path)

        if write_kwargs is None:
            write_kwargs = {}
        
        # write to file
        tiles_gdf.to_file(write_path, **write_kwargs)

    return tiles_gdf


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
        help='The size of the WBD. Options are 2, 4, 6, 8, 10, 12.',
        choices=[2, 4, 6, 8, 10, 12]
    )

    parser.add_argument(
        '--wbd_layer',
        '-l',
        type=str,
        required=False,
        default=None,
        help='The layer name of the WBD.'
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
        default=100,
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
        '--waive_overwrite_check',
        '-o',
        action='store_true',
        required=False,
        default=False,
        help='The overwrite check.'
    )

    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    tile_wbds(**kwargs)