#!/bin/bash

#####################################################################################################################
## Slurm implementation of fim_pipeline.sh 
##
## This top level submits slurm_pre_processing.sh, slurm_process_unit_wb.sh, & slurm_post_processing.sh as sbatch 
##      jobs.
##
## How to execute:
##      bash slurm_pipeline.sh /data/inputs/huc_lists/dev_small_test_4_huc.lst test_pipeline_wrapper
##
## **Note**
##      This slurm implementation has yet to scale beyond 4 HUC8s. Additional PW configurations and CSP availability
##      concerns still need to be addressed. 
##
## For Slurm Job dependencies, see:
##      https://slurm.schedmd.com/sbatch.html#OPT_dependency
#####################################################################################################################

huc_list=$1
run_name=$2

## Exit if incorrect arguments (here we need to provide a huclist that is accessible to the docker container)
if [ "${huc_list:0:5}" != "/data" ]
then
    printf "\nERROR: Provide a Huclist in format of /data/inputs/huc_lists, a single huc, or, for multiple HUCS "
    printf "use quotes and space delimited pattern as the first parameter\n"
    exit 1
fi
if [ "$2" = "" ]
then
    printf "\nERROR: Missing a run name argument as the second parameter\n"
    exit 1
fi

#####################################################################################################################
## Replace run_name in slurm_process_unit.sh with input argument ($2)
sed -i -e "s/placeholder/$run_name/g" slurm_process_unit_wb.sh

#####################################################################################################################
## Replace /data/ in huc_list argument to what is local on Controller Node's FileSystem (eg /fsx or /fimefs) 
## This depends on what was configured in cluster creation and what we provided to the 
## volume mount for the data directory 

relative_huc_list=${huc_list/data/efs}

#####################################################################################################################
## Split up the processing into 3 seperate logical processing steps (pre, compute, post)
## Each step correlates to a batch job/job array sent to the scheduler to ensure they are processed in the correct order
## We are making use of the sbatch --dependency option to wait for each step to finish before moving on to the next one 

## The SLURM_PRE_PROCESSING job sets up the folder structure and environment variables
## The SLURM_PROCESS_UNIT_WB job is an array job, which parallelizes the HUC8 level processing
## The post processing job runs the post processing steps (modifies rating curves, etc)

printf "\n Initiating fim_pre_processing job with a run_name of ${2} \n\t huc_list: ${1} \n"

SLURM_PRE_PROCESSING=$(sbatch --parsable --partition=pre-processing slurm_pre_processing.sh ${1} ${2})

printf "\n Jobs submitted, see appropriate SLURM.out files in directory where this script was run for details."
printf "\n\t Run squeue to view the job queue. \n\n"

## TODO - create another partition with a small VM for this one, as it just creates the job array
SLURM_PROCESS_UNIT_WB=$(sbatch --dependency=afterok:$SLURM_PRE_PROCESSING --parsable slurm_process_unit_wb.sh ${relative_huc_list})

printf "\n SLURM_PROCESS_UNIT_WB Submitted, Job ID is: $SLURM_PROCESS_UNIT_WB \n"

SLURM_PROCESS_UNIT_JOB_ARRAY_ID=$(($SLURM_PROCESS_UNIT_WB + 1))

printf "\n SLURM_PROCESS_UNIT_JOB_ARRAY_ID is: $SLURM_PROCESS_UNIT_JOB_ARRAY_ID \n"

## Wait about 5 minutes to let the pre processing complete, and let the scheduler submit the array job first
## There are no "futures" in slurm. The job id passed to --dependency must precede the current job id 
sleep 300

## post-processing compute c5.24xlarge - 96vCPU -> 48 CPU -2 (fim code requirement) = -j 46
bash slurm_post_processing.sh ${run_name} ${SLURM_PROCESS_UNIT_JOB_ARRAY_ID}

printf "\n slurm_post_processing.sh submitted, this depends on all of the array jobs completing. \n"
#####################################################################################################################

## Revert run_name arg back to placeholder
sed -i -e "s/$run_name/placeholder/g" slurm_process_unit_wb.sh

