#!/bin/bash

partition="$1"; shift
huc_array=("$@") 

echo "partition: $partition " 
echo "huc_array: ${huc_array[@]}"

## Create the Slurm script ($ used in script need to be escaped: \$)
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=placeholder
#SBATCH --output %x_%a.out # %x is the job-name, %a is the Job array ID (index) number.
#SBATCH --partition=compute_${partition}
##SBATCH --ntasks-per-node 10 # Use for more than single-node jobs
#SBATCH --ntasks=1
#SBATCH --cpus-per-task 14 # Use this for cores in single-node jobs.
#SBATCH --time=05:00:00
#SBATCH --array=0-$(( ${#huc_array[@]} - 1 ))

## Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

## mkdir -p /fsx/outputs_temp

## Get each individual HUC
HUC=\${huc_array[\$SLURM_ARRAY_TASK_ID]}
export HUC

## Get the run name
RUN_NAME=\${SLURM_JOB_NAME}

echo "Array job number: \$SLURM_ARRAY_TASK_ID"
echo "Running fim_process_unit_wb.sh on \${HUC}"
echo "RUN_NAME is \${RUN_NAME}"
echo "Container name is \${RUN_NAME}_\${HUC}"

## docker run --rm --name \${RUN_NAME}_\${HUC} -v /efs/repo/inundation-mapping/:/foss_fim -v /efs/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /efs/outputs_temp/:/fim_temp fim:latest ./foss_fim/fim_process_unit_wb.sh \${RUN_NAME} \${HUC}

docker run --rm --name \${RUN_NAME}_\${HUC} -v /efs/repo/inundation-mapping/:/foss_fim -v /efs/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /efs/outputs_temp/:/fim_temp fim:latest mkdir -p /outputs/\${RUN_NAME}/\${HUC}

EOF
