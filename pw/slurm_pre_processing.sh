#!/bin/bash

## Slurm wrapper of fim_pre_processing.sh

huc_list=$1
run_name=$2

#SBATCH --job-name=slurm_pre_processing
#SBATCH --output %x.out # %x is the ^^ slurm job-name
#SBATCH --partition=compute
##SBATCH --partition=pre-processing # This is set in PW Cluster Definition
#SBATCH --nodes=1
#SBATCH --cpus-per-task 1 # Use this for threads/cores in single-node jobs.
#SBATCH --time=00:10:00

# Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

docker run --rm --name fim_pre_processing  -v /efs/repo/inundation-mapping/:/foss_fim -v /fsx/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /fsx/outputs_temp/:/fim_temp robgpita/fim:fim_4 ./foss_fim/fim_pre_processing.sh -u "${huc_list}" -n "${run_name}" -jb 10

