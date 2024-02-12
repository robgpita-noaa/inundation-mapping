#!/bin/bash

#####################################################################################################################
## Slurm implementation of fim_pipeline.sh 
##
## How to execute:
##      bash slurm_pipeline.sh /data/inputs/huc_lists/dev_small_test_1_huc.lst test_pipeline_wrapper
##
## For Slurm Job dependencies, see:
##      https://slurm.schedmd.com/sbatch.html#OPT_dependency
#####################################################################################################################

huc_list=$1

## Exit if incorrect arguments (here we need to provide a huclist that is accessible to the docker container)
if [ "${huc_list:0:5}" != "/data" ]
then
    printf "\nERROR: Provide a Huclist in format of /data/inputs/huc_lists, a single huc, or for multiple, space delimineted, in quotes, as the first parameter\n"
    exit 1
fi
if [ "$2" = "" ]
then
    printf "\nERROR: Missing a run name argument as the second parameter\n"
    exit 1
fi

## Replace run_name in slurm_process_unit.sh with input argument ($2)
run_name=$2
sed -i -e "s/placeholder/$run_name/g" slurm_process_unit_wb.sh

#####################################################################################################################
## Replace /data/ in huc_list argument to what is local on Controller Node's FileSystem (eg /fsx or /fimefs) 
## This depends on what was configured in cluster creation and what we provided to the 
## volume mount for the data directory 

relative_huc_list=${huc_list/data/efs}
#####################################################################################################################

#####################################################################################################################
## Split up the processing into 3 seperate logical processing steps (pre, compute, post)
## Each step correlates to a batch job sent to the scheduler to ensure the are processed in the correct order
## We are using the --dependency flag to wait for each step to finish before moving on to the next one 
## The SLURM_PRE_PROCESSING job sets up the folder structure and environment variables
## The SLURM_PROCESS_UNIT_WB job is an array job, which parallelizes the HUC8 level processing
## The post processing job is not named here, however it runs the post processing steps (modifies rating curves, etc)

printf "\n Initiating fim_pre_processing job with ${2} as the run_name, and \n ${1} as the huc_list \n"

SLURM_PRE_PROCESSING=$(sbatch --parsable slurm_pre_processing.sh ${1} ${2})

SLURM_PROCESS_UNIT_WB=$(sbatch --parsable --dependency=afterok:$SLURM_PRE_PROCESSING slurm_process_unit_wb.sh ${relative_huc_list})

## post-processing compute c5.18xlarge - 72vCPU -> 36 CPU -2 (fim code requirement) = -j 34 
sbatch --parsable --dependency=afterany:$SLURM_PROCESS_UNIT_WB slurm_post_processing.sh -n ${run_name} -j 34
#####################################################################################################################

printf "Jobs submitted, see appropriate SLURM .out files for details \n Run squeue for job status. \n"

## Revert run_name arg back to placeholder
sed -i -e "s/$run_name/placeholder/g" slurm_process_unit_wb.sh

