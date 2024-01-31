#!/bin/bash

## Slurm wrapper of fim_process_unit_wb.sh
## Passed a huc list, this script will parallelize the submission of sbatch jobs to the scheduler

# Read number of lines in file supplied as argument
num_lines=$(wc -l $1 | awk '{print $1}')

# Create the Slurm script ($ used in script need to be escaped: \$)
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=placeholder
#SBATCH --output %x_%j.out # %x is the job-name, %j is the job id
#SBATCH --partition=compute
##SBATCH --ntasks-per-node 10 # Use for more than single-node jobs
#SBATCH --nodes=1
#SBATCH --cpus-per-task 10 # Use this for threads/cores in single-node jobs.
#SBATCH --time=01:30:00
#SBATCH --array=0-$(( num_lines - 1 ))

# Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

mkdir -p /fsx/outputs_temp

# Load huc_list.lst file into a bash array
readarray -t HUCS < $1

# Get each individual HUC
HUC=\${HUCS[\$SLURM_ARRAY_TASK_ID]}
export HUC

#  Get the run name
RUN_NAME=\${SLURM_JOB_NAME}

echo "Running fim_process_unit_wb.sh on \${HUC}"

echo "RUN_NAME is \${RUN_NAME}"


echo "Container name is \${RUN_NAME}_\${HUC}"

docker run --rm --name \${RUN_NAME}_\${HUC} -v /efs/repo/inundation-mapping/:/foss_fim -v /fsx/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /fsx/outputs_temp/:/fim_temp robgpita/fim:fim_4 ./foss_fim/fim_process_unit_wb.sh \${RUN_NAME} \${HUC}

EOF

