#!/bin/bash


##############################!/bin/bash -e

## SOURCE BASH FUNCTIONS
source $srcDir/bash_functions.env

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## ECHO PARAMETERS
# echo -e "memfree=$memfree"$stopDiv

## SET OUTPUT DIRECTORY FOR UNIT ##
#inputDataDir=/data/outputs/fim3_20210301_a92212d/12090301
#inputDataDir=/data/outputs/20210318_665f534_calibrated/12090301
inputDataDir=$1
outputDataDir=$inputDataDir/gms

# make outputs directory
if [ ! -d "$outputDataDir" ]; then
    mkdir -p $outputDataDir
else # remove contents if already exists
    rm -rf $outputDataDir
    mkdir -p $outputDataDir
fi

## TEMP ##
## SET VARIABLES AND FILE INPUTS ##
branch_id_attribute=levpa_id
branch_buffer_distance_meters=7000
ncores_gw=1
input_demThal=$inputDataDir/dem_thalwegCond.tif
input_flowdir=$inputDataDir/flowdir_d8_burned_filled.tif
input_slopes=$inputDataDir/slopes_d8_dem_meters.tif
input_demDerived_raster=$inputDataDir/demDerived_streamPixels.tif
input_demDerived_reaches=$inputDataDir/demDerived_reaches_split.gpkg
input_demDerived_reaches_points=$inputDataDir/demDerived_reaches_split_points.gpkg
input_demDerived_pixel_points=$inputDataDir/flows_points_pixels.gpkg
input_catchment_list=$inputDataDir/catchment_list.txt
input_stage_list=$inputDataDir/stage.txt
input_hydroTable=$inputDataDir/hydroTable.csv
input_src_full=$inputDataDir/src_full_crosswalked.csv
export startDiv="\n##########################################################################\n"
export stopDiv="\n##########################################################################"
ndv=-2147483648
hucNumber=12090301

## DERIVE LEVELPATH  ##
echo -e $startDiv"Generating Level Paths for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/derive_level_paths.py -i $input_demDerived_reaches -b $branch_id_attribute -o $outputDataDir/demDerived_reaches_levelPaths.gpkg -d $outputDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -v
Tcount

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/buffer_stream_branches.py -s $outputDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -i $branch_id_attribute -d $branch_buffer_distance_meters -b $outputDataDir/polygons.gpkg -v 
Tcount

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/clip_rasters_to_branches.py -b $outputDataDir/polygons.gpkg -i $branch_id_attribute -r $input_demThal $input_flowdir $input_slopes $input_demDerived_raster -c $outputDataDir/dem_thalwegCond.tif $outputDataDir/flowdir.tif $outputDataDir/slopes.tif $outputDataDir/demDerived.tif -v 
Tcount

##### EDIT DEM DERIVED POINTS TO ADD BRANCH IDS ######
echo -e $startDiv"EDITING DEM DERIVED POINTS for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/edit_points.py -i $outputDataDir/demDerived_reaches_levelPaths.gpkg -b $branch_id_attribute -r $input_demDerived_reaches_points -o $outputDataDir/demDerived_reaches_points.gpkg -p $outputDataDir/demDerived_pixels_points.gpkg
Tcount

## SUBSET VECTORS
echo -e $startDiv"Subsetting vectors to branches for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/query_vectors_by_branch_polygons.py -a $outputDataDir/polygons.gpkg -i $branch_id_attribute -s $outputDataDir/demDerived_reaches_levelPaths_dissolved.gpkg $outputDataDir/demDerived_reaches_points.gpkg $outputDataDir/demDerived_pixels_points.gpkg -o $outputDataDir/demDerived_reaches_levelPaths_dissolved.gpkg $outputDataDir/demDerived_reaches_points.gpkg $outputDataDir/demDerived_pixels_points.gpkg -v
Tcount

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create file of branch ids for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/generate_branch_list.py -t $input_hydroTable -c $outputDataDir/branch_id.lst -d $outputDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -b $branch_id_attribute
Tcount

## CREATE BRANCH LEVEL CATCH LISTS ##
echo -e $startDiv"Create branch level catch lists in HUC: $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/subset_catch_list_by_branch_id.py -c $inputDataDir/catchment_list.txt -s $outputDataDir/demDerived_reaches_levelPaths.gpkg -o $outputDataDir/catch_list.txt -b $branch_id_attribute -l $outputDataDir/branch_id.lst -v
Tcount

## LOOP OVER EACH STREAM BRANCH TO DERIVE BRANCH LEVEL HYDROFABRIC ##
for current_branch_id in $(cat $outputDataDir/branch_id.lst);
do

    #[ "$current_branch_id" -ne "15080120" ] && continue
    echo -e $startDiv$startDiv"Processing branch_id: $current_branch_id in HUC: $hucNumber ..."$stopDiv$stopDiv

    ## SPLIT DERIVED REACHES ##
    echo -e $startDiv"Split Derived Reaches $hucNumber"$stopDiv
    date -u
    Tstart
    $srcDir/split_flows.py $outputDataDir/demDerived_reaches_levelPaths_dissolved_$current_branch_id.gpkg $outputDataDir/dem_thalwegCond_$current_branch_id.tif $outputDataDir/demDerived_reaches_split_$current_branch_id.gpkg $outputDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg $inputDataDir/wbd8_clp.gpkg $inputDataDir/nwm_lakes_proj_subset.gpkg
    Tcount

    ## GAGE WATERSHED FOR PIXELS ##
    echo -e $startDiv"Gage Watershed for Pixels for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    Tstart
    mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_"$current_branch_id".tif -gw $outputDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputDataDir/demDerived_pixels_points_$current_branch_id.gpkg -id $outputDataDir/idFile_$current_branch_id.txt
    Tcount

    ## GAGE WATERSHED FOR REACHES ##
    echo -e $startDiv"Gage Watershed for Reaches for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    Tstart
    mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_$current_branch_id.tif -gw $outputDataDir/gw_catchments_reaches_$current_branch_id.tif -o $outputDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg -id $outputDataDir/idFile_$current_branch_id.txt
    Tcount

    # D8 REM ##
    echo -e $startDiv"D8 REM for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    Tstart
    $srcDir/rem.py -d $outputDataDir/dem_thalwegCond_"$current_branch_id".tif -w $outputDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputDataDir/rem_$current_branch_id.tif -t $outputDataDir/demDerived_$current_branch_id.tif
    Tcount

    ## BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS##
    echo -e $startDiv"Bring negative values in REM to zero and mask to catchments for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/rem_$current_branch_id.tif -B $outputDataDir/gw_catchments_reaches_$current_branch_id.tif --calc="(A*(A>=0)*(B>0))" --NoDataValue=$ndv --outfile=$outputDataDir/"rem_zeroed_masked_$current_branch_id.tif"
    Tcount

    ## POLYGONIZE REACH WATERSHEDS ##
    echo -e $startDiv"Polygonize Reach Watersheds for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    Tstart
    gdal_polygonize.py -8 -f GPKG $outputDataDir/gw_catchments_reaches_$current_branch_id.tif $outputDataDir/gw_catchments_reaches_$current_branch_id.gpkg catchments HydroID
    Tcount

    ## MASK SLOPE TO CATCHMENTS ##
    echo -e $startDiv"Mask to slopes to catchments for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/slopes_$current_branch_id.tif -B $outputDataDir/gw_catchments_reaches_$current_branch_id.tif --calc="A*(B>0)" --NoDataValue=$ndv --outfile=$outputDataDir/slopes_masked_$current_branch_id.tif
    Tcount

    ## HYDRAULIC PROPERTIES ##
    echo -e $startDiv"Sample reach averaged parameters for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    Tstart
    $taudemDir/catchhydrogeo -hand $outputDataDir/rem_zeroed_masked_$current_branch_id.tif -catch $outputDataDir/gw_catchments_reaches_$current_branch_id.tif -catchlist $outputDataDir/catch_list_$current_branch_id.txt -slp $outputDataDir/slopes_masked_$current_branch_id.tif -h $input_stage_list -table $outputDataDir/src_base_$current_branch_id.csv
    Tcount

    ## FINALIZE CATCHMENTS AND MODEL STREAMS ##
    echo -e $startDiv"Finalize catchments and model streams for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    date -u
    Tstart
    $srcDir/gms/finalize_srcs.py -b $outputDataDir/src_base_$current_branch_id.csv -w $input_hydroTable -r $outputDataDir/src_full_$current_branch_id.csv -f $input_src_full -t $outputDataDir/hydroTable_$current_branch_id.csv
    Tcount

    # make branch output directory and mv files to
    branchOutputDir=$outputDataDir/$current_branch_id
    if [ ! -d "$branchOutputDir" ]; then
        mkdir -p $branchOutputDir
    fi
    
    # mv files to branch output directory
    find $outputDataDir -maxdepth 1 -type f -iname "*_$current_branch_id.*" -exec mv {} $branchOutputDir \;

done

# TEMP: Remove files that are not in catchlist
# rasters
find $outputDataDir -maxdepth 1 -type f -iname "dem_thalwegCond_*.tif" -delete
find $outputDataDir -maxdepth 1 -type f -iname "flowdir_*.tif" -delete
find $outputDataDir -maxdepth 1 -type f -iname "slopes_*.tif" -delete
find $outputDataDir -maxdepth 1 -type f -iname "demDerived_*.tif" -delete

# vectors
find $outputDataDir -maxdepth 1 -type f -iname "demDerived_reaches_*.gpkg" -delete
find $outputDataDir -maxdepth 1 -type f -iname "demDerived_reaches_points_*.gpkg" -delete
find $outputDataDir -maxdepth 1 -type f -iname "demDerived_pixels_points_*.gpkg" -delete

# other
find $outputDataDir -maxdepth 1 -type f -iname "idFile_*.txt" -delete