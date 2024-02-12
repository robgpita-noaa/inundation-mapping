#!/bin/bash

#####################################################################################################################
##
## Slurm wrapper of fim_pre_processing.sh
##
## How to execute:
##      sbatch slurm_pre_processing.sh /data/inputs/huc_lists/dev_small_test_1_huc.lst test_pipeline_steps
#####################################################################################################################

huc_list=$1
run_name=$2

#SBATCH --job-name=slurm_pre_processing
#SBATCH --output %x.out # %x is the ^^ slurm job-name
##SBATCH --partition=compute
#SBATCH --partition=pre-processing # This is set in PW Cluster Definition
#SBATCH --nodes=1
#SBATCH --cpus-per-task 1 # Use this for threads/cores in single-node jobs.
#SBATCH --time=00:10:00

## Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

docker run --rm --name fim_pre_processing -v /efs/repo/inundation-mapping/:/foss_fim -v /efs/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /efs/outputs_temp/:/fim_temp robgpita/fim:fim_4 ./foss_fim/fim_pre_processing.sh -u "${huc_list}" -n "${run_name}" -jb 14
