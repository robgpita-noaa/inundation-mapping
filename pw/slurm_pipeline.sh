#!/bin/bash

## Slurm implementation of fim_pipeline.sh 

huc_list=$1

# Exit if incorrect arguments (here we need to provide a huclist that is accessible to the docker container)
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

# Replace run_name in slurm_process_unit.sh with input argument ($2)
run_name=$2 
sed -i -e "s/placeholder/$run_name/g" slurm_process_unit_wb.sh


# Replace /data/ in huc_list argument to what is local on Controller Node's FileSystem
# For example /fsx - Depends on what was configured in cluster creation and what we provided to the 
# volume mount for the data directory 

relative_huc_list=${huc_list/data/fsx}



printf "\n Initiating fim_pre_processing job with ${2} as the run_name, and \n ${1} as the huc_list \n"


SLURM_PRE_PROCESSING=$(sbatch --parsable slurm_pre_processing.sh ${1} ${2}) 


SLURM_PROCESS_UNIT_WB=$(sbatch --parsable --dependency=afterok:$SLURM_PRE_PROCESSING slurm_process_unit_wb.sh ${relative_huc_list})



#sbatch --parsable --dependency=afterok:$SLURM_PROCESS_UNIT_WB slurm_post_processing.sh -n ${run_name} -j 20



printf "Submitted jobs, see appropriate SLURM .out files for details \n Run squeue for job status."


# Revert run_name arg back to placeholder
sed -i -e "s/$run_name/placeholder/g" slurm_process_unit_wb.sh

