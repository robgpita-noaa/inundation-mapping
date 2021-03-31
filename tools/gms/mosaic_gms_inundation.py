#!/usr/bin/env python
# coding: utf-8

from glob import glob
from gms.overlapping_inundation import OverlapWindowMerge
import argparse
from os.path import join

def Mosaic_gms_inundation(inundation_dir,mosaic=None,mask=None,nodata=-2147483647,workers=4):
    
    inundation_maps = glob( join(inundation_dir,'*.tif'))
    
    overlap = OverlapWindowMerge( inundation_maps, (30, 30) )

    if mosaic is not None:
        overlap.merge_rasters(mosaic, threaded=True, workers=4,nodata=nodata)

    if mask:
        overlap.mask_mosaic(mosaic,mask,outfile=mosaic)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mosaic GMS Inundation Rasters')
    parser.add_argument('-i','--inundation-dir', help='Inundation Maps', required=True)
    parser.add_argument('-n','--nodata', help='Inundation Maps', required=False,default=-2147483647)
    parser.add_argument('-w','--workers', help='Number of Workers', required=False,default=4)
    parser.add_argument('-m','--mosaic', help='Mosaiced inundation Maps', required=False,default=None)

    args = vars(parser.parse_args())
    
    Mosaic_gms_inundation(**args)