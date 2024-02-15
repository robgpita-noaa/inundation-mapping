#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Acquires and preprocesses 3DEP DEMs for use with HAND FIM.

TODO:
    - implement logging

Command Line Usage:
    /foss_fim/data/usgs/get_3dep_static_tiles.py -t <your_1m_tile_index> <your_3m_tile_index> -d /data/inputs/3dep_dems/1m_5070_lidar_tiles -j <some_worker_number>
"""

from __future__ import annotations
from typing import List, Sequence
from numbers import Number
from pyproj import CRS

import uuid
import argparse
import os
from pathlib import Path
from functools import partial

from osgeo import gdal
from rasterio.enums import Resampling
import pandas as pd
import geopandas as gpd
import odc.geo.xr
from dotenv import load_dotenv
from dask.distributed import Client, as_completed, get_client, LocalCluster
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


def _retrieve_process_write_single_3dep_dem_tile(
    url : str,
    dem_resolution : Number,
    crs : str | CRS,
    ndv : Number,
    dem_tile_dir : str,
    write_kwargs : dict,
    write_ext : str,
    completed_tiles_fn : str,
    overwrite : bool
) -> str:
    """
    Retrieves and processes a single 3DEP DEM tile.
    """

    # create write path
    url_split = url.split('/')
    project_name = url_split[-3]
    tile_name = url_split[-1].split('.')[0]

    # construct file name
    dem_file_name = os.path.join(dem_tile_dir,f'{project_name}___{tile_name}.{write_ext}')

    # open completed tile list
    with open(completed_tiles_fn, 'r') as f:
        completed_tiles = set(f.read().splitlines())
 
    # check if file exists and return if not overwriting
    if (dem_file_name in completed_tiles) & os.path.exists(dem_file_name):
        if overwrite:
            os.remove(dem_file_name)
        else:
            return dem_file_name            

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

        # write file
        dem.rio.to_raster(
            dem_file_name,
            **write_kwargs
        )

    # write to completed tiles
    with open(completed_tiles_fn, 'a') as f:
        f.write(dem_file_name + '\n')

    return dem_file_name


def get_3dep_static_tiles(
    dem_3dep_dir : str | Path,
    tile_index : str | Path | gpd.GeoDataFrame | Sequence[str | Path | gpd.GeoDataFrame],
    write_kwargs : dict = WRITE_KWARGS,
    write_ext : str = 'tif',
    crs : str | CRS = DEFAULT_FIM_PROJECTION_CRS,
    ndv : Number = -999999,
    overwrite : bool = False,
    max_retries : int = MAX_RETRIES
) -> List[str | Path]:
    """
    Acquires and preprocesses 3DEP DEM tiles for use with HAND FIM.

    Parameters
    ----------
    dem_3dep_dir : str or Path
        Path to 3DEP DEM directory outputs.
    tile_index : str or Path or gpd.GeoDataFrame or Sequence of str or Path or gpd.GeoDataFrame
        Path to tile index or tile index as GeoDataFrame. Also, accepts a sequence of paths or GeoDataFrames. Tile indices are constructed with `gdaltindex`. Must contain 'location' and 'dem_resolution' columns.
    write_kwargs : dict, default = WRITE_KWARGS
        Write kwargs for tiles.
    write_ext : str, default = 'tif'
        Write extension for tiles.
    crs : str | CRS, default = DEFAULT_FIM_PROJECTION_CRS
        Target desired CRS for tiles.
    ndv : Number, default = -999999
        No data value for tiles.
    overwrite : bool, default = False
        Overwrite existing tiles.
    max_retries : int, default = MAX_RETRIES
        Max retries for each tile.

    Returns
    -------
    List of str or Path
        Path to VRT file. Returns str if dem_tile_dir is a str, and Path if dem_tile_dir is a Path.

    Raises
    ------
    ValueError
        If no 3DEP DEMs were retrieved.
    """
    
    # parent directory location
    os.makedirs(dem_3dep_dir, exist_ok=True)
    
    # create tiles directory
    dem_tile_dir = os.path.join(dem_3dep_dir, 'tiles')
    os.makedirs(dem_tile_dir, exist_ok=True)

    # completed tiles file
    completed_tiles_fn = os.path.join(dem_3dep_dir, 'processed_tiles.lst')
    
    # create completed tiles file if not exists or overwrite
    if (not os.path.exists(completed_tiles_fn)) | (overwrite):
        # just create the file with no contents
        with open(completed_tiles_fn, 'w') as f:
            pass

    # load tiles
    if isinstance(tile_index, (str, Path)):
        tile_index = gpd.read_file(tile_index)
    elif isinstance(tile_index, (gpd.GeoDataFrame)):
        pass
    elif isinstance(tile_index, (Sequence)):
        tile_index = pd.concat([gpd.read_file(tile_fn) for tile_fn in tile_index])
    else:
        raise ValueError("tile_index must be a str, Path, GeoDataFrame, or Sequence[str, Path, GeoDataFrame]")
    
    # sort tile_index based on order of dem_resolution
    tile_index = tile_index.sort_values(by='dem_resolution', ignore_index=True)
    
    # number of inputs
    num_of_inputs = len(tile_index)

    # create partial function for 3dep acquisition
    _retrieve_process_write_single_3dep_dem_tile_partial = partial(
        _retrieve_process_write_single_3dep_dem_tile,
        crs = crs,
        ndv = ndv,
        dem_tile_dir = dem_tile_dir,
        write_kwargs = write_kwargs,
        write_ext = write_ext,
        overwrite = overwrite,
        completed_tiles_fn = completed_tiles_fn
    )

    # debug
    # tile_index = tile_index.head(10)

    # get dask client, if not available, download serially
    try:
        
        client = get_client()
    
    # download tiles serially since client is not available
    except ValueError:

        print("Dask client not available, downloading 3DEP DEMs serially ...")
        
        res_and_tile_fn_tuple = [(None, None)] * num_of_inputs
        for i, rows in tqdm(tile_index.iterrows(), desc="Downloading 3DEP DEMs by tile", total=num_of_inputs):
            
            # get inputs
            url, res = rows['location'], rows['dem_resolution']
            
            # retrieve, process, and write 3dep dem tile
            try:
                tile_fn = _retrieve_process_write_single_3dep_dem_tile_partial(url, res)
            except Exception as e:
                print(f"Failed to retrieve, process, and write 3DEP DEM tile: {url}")
                pass
            else:
                res_and_tile_fn_tuple[i] = (res, tile_fn)

    # use dask client
    else:

        print(f"Downloading 3DEP DEMs tiles using Dask {client} ...")

        # submit futures
        futures = [
            client.submit(
                _retrieve_process_write_single_3dep_dem_tile_partial, row['location'], row['dem_resolution']
            ) 
            for _, row in tile_index.iterrows()
        ]

        # Dictionary to keep track of retries
        retries = {future: 0 for future in futures}

        # create pbar
        pbar = tqdm(total=len(futures), desc=f"Downloading 3DEP DEM tiles")

        # Loop through the futures, checking for exceptions and resubmitting the task if necessary
        res_and_tile_fn_tuple = [(None, None)] * len(futures)
        for future in as_completed(futures):
            # Get the index of the future
            idx = futures.index(future)
            
            try:
                tile_fn = future.result()
            except:
                # Find the original arguments used for the failed future
                url, res = tile_index.loc[idx, ['location', 'dem_resolution']]
                
                if retries[future] < max_retries:
                    
                    # Print a message indicating that the task is being retried
                    print(f"Retrying to retrieve, process, and write 3DEP DEM tile: {url}")
                    
                    # Increment the retry count for this future
                    retries[future] += 1
                    
                    # Resubmit the task directly using client.submit
                    new_future = client.submit(_retrieve_process_write_single_3dep_dem_tile_partial, url, res)
                    
                    # Replace the failed future with the new future in the list and update the retries dictionary
                    futures[idx] = new_future
                    retries[new_future] = retries[future]
                
                else:
                    # If the maximum number of retries has been reached, print an error message
                    print(f"Failed to retrieve, process, and write 3DEP DEM tile: {url}")

            finally:
                res = tile_index.loc[idx, 'dem_resolution']
                res_and_tile_fn_tuple[idx] = (res, tile_fn)

                pbar.update(1)

        # close pbar
        pbar.close()

    # remove None values
    res_and_tile_fn_tuple = [
        res_tile for res_tile in res_and_tile_fn_tuple if (res_tile[0] is not None) & (res_tile[1] is not None)
    ]

    # raise error if no dems were retrieved
    if len(res_and_tile_fn_tuple) == 0:
        raise ValueError("No 3DEP DEMs were retrieved")
    
    # sort by increasing resolution
    res_and_tile_fn_tuple = sorted(res_and_tile_fn_tuple, key=lambda x: x[0])

    # get dem_tile_file_names
    dem_tile_file_names = [tile_fn for _, tile_fn in res_and_tile_fn_tuple]

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
    dem_resolution = kwargs.pop('dem_resolution')

    # acquire and preprocess 3dep dems
    #with Client(n_workers=num_workers, threads_per_worker=1) as client:
    with LocalCluster(n_workers=num_workers, threads_per_worker=1, memory_limit=None) as cluster:
        with Client(cluster) as client:
            dem_tile_file_names = get_3dep_static_tiles(**kwargs)

    # create vrt
    kwargs = { k : kwargs[k] for k in ['dem_3dep_dir','ndv']}
    kwargs['ten_m_vrt'] = ten_m_vrt
    kwargs['dem_resolution'] = dem_resolution

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
        required=True,
        nargs='+'
    )

    parser.add_argument(
        '-r', '--dem-resolution',
        help='DEM resolution of VRT file in meters',
        required=False,
        default=1
    )

    parser.add_argument(
        '-w', '--write-kwargs',
        help='GDAL write options for tiles',
        type=dict,
        default=WRITE_KWARGS,
        required=False
    )

    parser.add_argument(
        '-e', '--write-ext',
        help='Write file extension for tiles',
        type=str,
        default='tif',
        required=False
    )

    parser.add_argument(
        '-o', '--overwrite',
        help='Overwrite existing tiles',
        default=False,
        action='store_true',
        required=False
    )

    parser.add_argument(
        '-c', '--crs',
        help='Desired CRS',
        type=str,
        default=DEFAULT_FIM_PROJECTION_CRS,
        required=False
    )

    parser.add_argument(
        '-n', '--ndv',
        help='Desired no data value for tiles',
        type=float,
        default=-999999,
        required=False
    )

    parser.add_argument(
        '-v', '--ten-m-vrt',
        help='Path to existing 10m VRT file',
        type=str,
        default=TEN_M_VRT,
        required=False
    )

    parser.add_argument(
        '-j', '--num-workers',
        help='Number of workers for dask client',
        type=int,
        default=NUM_WORKERS,
        required=False
    )

    parser.add_argument(
        '-m', '--max-retries',
        help='Max retries for each tile',
        type=int,
        default=MAX_RETRIES,
        required=False
    )
    
    # Extract to dictionary and assign to variables.
    kwargs = vars(parser.parse_args())

    # Run main function.
    seamless_vrt_fn = main(kwargs)