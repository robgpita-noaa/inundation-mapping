#!/bin/bash

#####################################################################################################################
##      This script is untested and a work in progress. 
##
## Slurm wrapper of fim_process_unit_wb.sh
## Passed a huc list, this script will parallelize the submission of sbatch jobs to the scheduler
## 
## The key difference between this script and slurm_process_unit_wb.sh is the functionality to split a larger list of
##      of hucs into multiple compute partitions to avoid resource limitations in cloud computing environments. 
##
## This script must be run AFTER the pre_processing step has completed!
## Do not modify the SBATCH commands in this file.
##
## How to execute:
##      sbatch slurm_partition_process_unit.sh /data/inputs/huc_lists/dev_small_test_4_huc.lst 4
#####################################################################################################################

## Set the amount of partitions
partitions=$1

## Read number of lines in file supplied as argument
num_lines=$(wc -l $2 | awk '{print $1}')

## Get all HUCS into one array
readarray -t HUCS < $2

# Calculate the size of chunks
chunkSize=$(( ${#HUCS[@]} / partitions ))
remainder=$(( ${#HUCS[@]} % partitions ))

echo "chunkSize -> ${chunkSize}"
echo "remainder -> ${remainder}" 

## Create the subsets arrays of hucs based on amount of partitions
for ((i=0; i<partitions; i++)); do
    start=$((i * chunkSize))
    end=$((start + chunkSize))
    eval "chunked_array_of_hucs_$i=('${HUCS[@]:${start}:${chunkSize}}')"    
done

## Handle the remainder
if [ $remainder -gt 0 ]; then
    start=$((chunkSize * partitions))
    eval "chunked_array_of_hucs_$partitions=('${HUCS[@]:${start}:${remainder}}')"
fi

## Depending on the remainder, iterate over all chunked arrays
if [ $remainder -gt 0 ]; then
    for ((i=0; i<=partitions; i++)); do
        eval "echo chunked_array_of_hucs_$i: \${chunked_array_of_hucs_$i[@]}"
        # eval "sbatch process_unit_wb_array.slurm \$i \${chunked_array_of_hucs_$i[@]}"
    done
else 
    for ((i=0; i<partitions; i++)); do
        eval "echo chunked_array_of_hucs_$i: \${chunked_array_of_hucs_$i[@]}"
        # eval "sbatch process_unit_wb_array.slurm \$i \${chunked_array_of_hucs_$i[@]}"
    done
fi
