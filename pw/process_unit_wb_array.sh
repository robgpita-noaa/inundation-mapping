#!/bin/bash

# Set arguments to variables
while [ "$1" != "" ]; do
    case $1 in
        -p|--partition)
            shift
            partition=$1
            ;;
        -n|--runName)
            shift
            runName=$1
            ;;
        -u|--hucList)
            shift
            huc_array=("$@")
            ;;
        *) ;;
    esac
    shift
done

echo "partition: $partition " 
echo "huc_array: ${huc_array[@]}"
echo "huc_array length: ${#huc_array[@]}"

## docker run --rm --name \${HUC} -v /efs/repo/inundation-mapping/:/foss_fim -v /efs/inputs/:/data/inputs -v /efs/outputs/:/outputs -v /efs/outputs_temp/:/fim_temp fim:latest ./foss_fim/fim_process_unit_wb.sh \${runName} \${HUC}

## Create the Slurm script ($ used in script need to be escaped: \$)
sbatch <<EOF
#!/bin/bash

#SBATCH --job-name="${runName}${partition}"
#SBATCH --output %x_%a.out # %x is the job-name, %a is the Job array ID (index) number.
#SBATCH --partition=compute_$partition
#SBATCH --ntasks=1
#SBATCH --cpus-per-task 14 # Use this for cores in single-node jobs.
#SBATCH --time=05:00:00
#SBATCH --array=0-$(( ${#huc_array[@]} - 1 ))
##SBATCH --ntasks-per-node 10 # Use for more than single-node jobs

## Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

echo "huc_array: ${huc_array[@]}"
echo "huc_array[0]: ${huc_array[0]}"
echo "huc_array[1]: ${huc_array[1]}"
echo "huc_array[\$SLURM_ARRAY_TASK_ID]: ${huc_array[\${SLURM_ARRAY_TASK_ID}]}"


## Get each individual HUC
# HUC=${huc_array[\$SLURM_ARRAY_TASK_ID]}
# echo "HUC (w/o {})-> \${HUC}"
HUC=${huc_array[\${SLURM_ARRAY_TASK_ID}]}
echo "HUC (w {})-> \${HUC2}"

echo "Array job number: \${SLURM_ARRAY_TASK_ID}"
echo "Running fim_process_unit_wb.sh on: ${HUC}"
echo "Running fim_process_unit_wb.sh on: \${HUC}"
echo "runName is \${runName}"
echo "Container name is \${HUC}"

# docker run --rm --name \${HUC} \
# -v /efs/repo/inundation-mapping/:/foss_fim \
# -v /efs/inputs/:/data/inputs \
# -v /efs/outputs/:/outputs \
# -v /efs/outputs_temp/:/fim_temp fim:latest \
# mkdir -p /outputs/\${runName}/\${HUC}

EOF
