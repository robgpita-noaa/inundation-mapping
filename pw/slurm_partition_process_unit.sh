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

## Read number of lines in file supplied as argument
num_lines=$(wc -l $1 | awk '{print $1}')

## Set the amount of partitions
partitions=$2

## Get all HUCS into one array
readarray -t HUCS < $1

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
    done
else 
    for ((i=0; i<partitions; i++)); do
        eval "echo chunked_array_of_hucs_$i: \${chunked_array_of_hucs_$i[@]}"
    done
fi

# # First Chunk
# ${HUCS[@]:0:42}
# # Second Chunk
# ${HUCS[@]:42:42}
# # Third Chunk
# ${HUCS[@]:84:42}
# # Fourth Chunk
# ${HUCS[@]:126:43}

## Create the Slurm script ($ used in script need to be escaped: \$)
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=placeholder
#SBATCH --output %x_%a.out # %x is the job-name, %a is the Job array ID (index) number.
#SBATCH --partition=compute
##SBATCH --ntasks-per-node 10 # Use for more than single-node jobs
#SBATCH --ntasks=1
#SBATCH --cpus-per-task 14 # Use this for cores in single-node jobs.
#SBATCH --time=05:00:00
#SBATCH --array=0-$(( num_lines - 1 ))

## Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

## mkdir -p /fsx/outputs_temp

## Load huc_list.lst file into a bash array
readarray -t HUCS < $1

## Get each individual HUC
HUC=\${HUCS[\$SLURM_ARRAY_TASK_ID]}
export HUC

## Get the run name
RUN_NAME=\${SLURM_JOB_NAME}

echo "Running fim_process_unit_wb.sh on \${HUC}"
echo "RUN_NAME is \${RUN_NAME}"
echo "Container name is \${RUN_NAME}_\${HUC}"

docker run --rm --name \${RUN_NAME}_\${HUC} -v /efs/repo/inundation-mapping/:/foss_fim -v /efs/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /efs/outputs_temp/:/fim_temp fim:latest ./foss_fim/fim_process_unit_wb.sh \${RUN_NAME} \${HUC}

EOF
