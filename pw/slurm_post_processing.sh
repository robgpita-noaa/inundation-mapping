#!/bin/bash

## Slurm wrapper of fim_post_processing.sh

run_name=$1

#SBATCH --job-name=slurm_post_processing
#SBATCH --output slurm_post_processing_%j.out 
#SBATCH --partition=compute
##SBATCH --partition=post-processing # This is set in PW Cluster Definition
#SBATCH --nodes=1

## vCPU is not the same as CPU. Physical CPU is checked in the fim code via python os.cpu_count() - 2.
## If unsure, provision a compute node interactively and issue the 'lscpu' command to verify 
## available physical cores. Subtract 2 from that number, and provide as argument to --cpus-per-task below.

#SBATCH --cpus-per-task 14 # Use this for threads/cores in single-node jobs.
#SBATCH --time=04:00:00

# Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

docker run --rm --name fim_post_processing  -v /efs/repo/inundation-mapping/:/foss_fim -v /fsx/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /fsx/outputs_temp/:/fim_temp robgpita/fim:fim_4 ./foss_fim/fim_post_processing.sh -n "${run_name}" -j 14

