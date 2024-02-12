#!/bin/bash

## Use this script for a single submission of fim_pipeline.sh 

#####################################################################################################################
## The use case would be 1 HUC8, or one very large compute node to process hucs sequentially
##
## This job is meant to be configured within this script to meet needs of a particular use case
##     Please modify:
##          SBATCH parameters
##          Arguments to fim_pipeline.sh at the end of the docker run command
##
##  Example:
##      sbatch slurm_single_fim_pipeline.sh
#####################################################################################################################

## The --job-name is transferred to the run_name (output directory)
#SBATCH --job-name=slurm_single_fim_pipeline_test 
#SBATCH --output %x.out # %x is the job-name
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --cpus-per-task 10 # Use this for threads/cores in single-node jobs.
#SBATCH --time=06:00:00

# Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

# Make temporary outputs directory if utilizing ephemeral FileSystem
# mkdir -p /fsx/outputs_temp

RUN_NAME=${SLURM_JOB_NAME}

# Spin up Docker container with correct mounts, and issue fim_pipeline.sh
docker run --rm --name ${RUN_NAME} -v /efs/repo/inundation-mapping/:/foss_fim -v /efs/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /efs/outputs_temp/:/fim_temp robgpita/fim:fim_4 ./foss_fim/fim_pipeline.sh -u 05030104 -n ${RUN_NAME} -jh 1 -jb 10 -o


