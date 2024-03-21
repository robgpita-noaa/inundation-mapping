#!/bin/bash

#####################################################################################################################
## Slurm implementation of fim_pipeline.sh 
##
## This top level submits slurm_pre_processing.sh, slurm_process_unit_wb.sh, & slurm_post_processing.sh as sbatch 
##      jobs.
##
## How to execute:
##      bash slurm_pipeline.sh -u /data/inputs/huc_lists/dev_small_test_4_huc.lst -n test_slurm_pipeline
##
## **Note**
##      This slurm implementation has yet to scale beyond 4 HUC8s. Additional PW configurations and CSP availability
##      concerns still need to be addressed. 
##
## For Slurm Job dependencies, see:
##      https://slurm.schedmd.com/sbatch.html#OPT_dependency
#####################################################################################################################

:
usage()
{
    echo "
    Processing of HUC's comes in three steps. You can run 'slurm_pipeline.sh' which will run the three main scripts: 
        'slurm_pre_processing.sh', 'slurm_process_unit_wb.sh'/'slurm_partition_process_unit.sh' & 'slurm_post_processing.sh'.
        These scripts are wrappers of : 'fim_pre_processing.sh', 'fim_process_unit_wb.sh' & 'fim_post_processing.sh'.

    Usage : slurm_pipeline.sh -u <huc8> -n <name_of_your_run>

    All arguments to this script are passed to 'fim_pre_processing.sh'.
    REQUIRED:
      -u/--hucList      : HUC8s to run; more than one HUC8 should be passed in quotes (space delimited).
                            A line delimited file, with a .lst extension, is also acceptable.
                            HUC8s must be present in inputs directory.
      -n/--runName      : A name to tag the output directories and log files (only alphanumeric).

    OPTIONS:
      -h/--help         : Print usage statement.
      -jh/--jobLimit    : Max number of concurrent HUC jobs to run. Default 1 job at time.
      -jb/--jobBranchLimit
                        Max number of concurrent Branch jobs to run. Default 1 job at time.
                        - Note: Make sure that the product of jh and jb plus 2 (jh x jb + 2)
                            does not exceed the total number of cores available.
      -p/--partitions   : The amount of partitions available. Used to 'chunk' the huc list, into a subset of arrays to
                            submit them into different partitions.
                            Before the -p argument is supplied, we need to do a little bit of math. First, it is necessary
                            to know how many HUCs are in the file you're submitting (wc -l <huc_list>.lst). 
                            Based off of that number, ideally you should provide the --partition that is evenly divisible 
                            (or as close as possible) by the amount of HUCs. If there is a remainder, there will be another
                            chunked huc array containing the remaining hucs. Be advised that there will be an additional 
                            partition that is needed (+1 of whatever argument provided) to run the remainder. 
                            If there is a remainder, provide a value to the --partition argument which is one less than the 
                            available partitions in the cluster. Take the following exmaples:
                              list_of_10.lst has 10 HUCs, if you provide a -p of 2, there will be five hucs in each array, 
                                    submitted to 2 compute partitions. :)
                              list_of_10.lst has 10 HUCs, if you provide a -p of 3, there will be 4 huc arrays submitted to
                                    4 compute partitions. 
                                   3 arrays with 3 HUCS, and one array comprising the remaining HUC, totalling 4 arrays and 4 partitions.
      -o                : Overwrite outputs if they already exist.
      -s/--skipPost         : If this param is included, the post processing step will be skipped.


    Running 'slurm_pipeline.sh' is a quicker process than running all three scripts independently; however,
        you can run each slurm script them independently if you like. 
    "
    exit
}

# print usage if agrument is '-h' or '--help'
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

while [ "$1" != "" ]; do
    case $1 in
        -u|--hucList)
            shift
            hucList=$1
            ;;
        -n|--runName)
            shift
            runName=$1
            ;;
        -jb|--jobBranchLimit)
            shift
            jobBranchLimit=$1
            ;;
        -p|--partitions)
            shift
            partitions=$1
            ;;
        -h|--help)
            shift
            usage
            ;;
        -o)
            overwrite=1
            ;;
        -s|--skipPost)
            skipPost=1
            ;;
        *) ;;
    esac
    shift
done

# Print usage if arguments empty
if [ "$hucList" = "" ]; then
    echo "ERROR: Missing -u hucList argument"
    usage
    exit 1
fi
if [ "$runName" = "" ]; then
    echo "ERROR: Missing -n runName argument"
    usage
    exit 1
fi

# Default values
if [ "$jobBranchLimit" = "" ]; then jobBranchLimit=1; fi
if [ -z "$partitions" ]; then partitions=0; fi
if [ -z "$overwrite" ]; then overwrite=0; fi
if [ -z "$skipPost" ]; then skipPost=0; fi

## Exit if incorrect arguments (here we need to provide a huclist that is accessible to the docker container)
if [ "${hucList:0:5}" != "/data" ]; then
    printf "\nERROR: Provide a Huclist in format of /data/inputs/huc_lists, a single huc, or, for multiple HUCS "
    printf "use quotes and space delimited pattern as the first parameter\n"
    exit 1
fi

#####################################################################################################################
## Replace runName in slurm_process_unit.sh with $runName argument
sed -i -e "s/placeholder/$runName/g" slurm_process_unit_wb.sh

#####################################################################################################################
## Replace /data/ in huc_list argument to what is local on Controller Node's FileSystem (eg /fsx, /fimefs, etc) 
## This depends on what was configured in cluster creation and what we provided to the 
## volume mount for the data directory 

relativeHucList=${hucList/data/efs}

#####################################################################################################################
## Split up the processing into 3 seperate logical processing steps (pre, compute, post)
## Each step correlates to a batch job/job array sent to the scheduler to ensure they are processed in the correct order
## We are making use of the sbatch --dependency option to wait for each step to finish before moving on to the next one 

## The SLURM_PRE_PROCESSING job sets up the folder structure and environment variables
## The SLURM_PROCESS_UNIT_WB job is an array job, which parallelizes the HUC8 level processing
## The post processing job runs the post processing steps (modifies rating curves, etc)

printf "\n Initiating fim_pre_processing job with a runName of $runName \n\t hucList: $hucList \n"

SLURM_PRE_PROCESSING=$(sbatch --parsable --partition=pre-processing slurm_pre_processing.sh $hucList $runName $jobBranchLimit $overwrite)

printf "\n Jobs submitted, see appropriate SLURM.out files in directory where this script was run for details."
printf "\n\t Run squeue to view the job queue. \n"

## TODO -> create another partition to process the array jobs and specify it below --partition=process_unit_array

## If there is partition argument issue the correct script
if [ $partitions -eq 0 ]; then
    SLURM_PROCESS_UNIT_WB=$(sbatch --partition=process_unit_array --dependency=afterok:$SLURM_PRE_PROCESSING --parsable slurm_process_unit_wb.sh ${relativeHucList})
    printf "\n SLURM_PROCESS_UNIT_WB Submitted, Job ID is: $SLURM_PROCESS_UNIT_WB \n"
    SLURM_PROCESS_UNIT_JOB_ARRAY_ID=$(($SLURM_PROCESS_UNIT_WB + 1))
    printf "\n SLURM_PROCESS_UNIT_JOB_ARRAY_ID is: $SLURM_PROCESS_UNIT_JOB_ARRAY_ID \n"
else
    SLURM_PROCESS_UNIT_WB_ARRAY=$(sbatch --partition=process_unit_array --dependency=afterok:$SLURM_PRE_PROCESSING --parsable slurm_partition_process_unit.sh ${partitions} ${runName} ${relativeHucList})
    printf "\n SLURM_PROCESS_UNIT_WB_ARRAY Submitted, Job ID is: $SLURM_PROCESS_UNIT_WB_ARRAY \n"
    SLURM_PROCESS_UNIT_JOB_ARRAY_IDS=($SLURM_PROCESS_UNIT_WB_ARRAY:)
    for ((i=0; i<partitions; i++)); do
        SLURM_PROCESS_UNIT_JOB_ARRAY_IDS+=$(($SLURM_PROCESS_UNIT_WB_ARRAY + $i + 1))
        if [ $i -lt $(( $partitions - 1 )) ];then
            SLURM_PROCESS_UNIT_JOB_ARRAY_IDS+=":"
        fi
    done
fi

## TODO:  Handle multiple slurm jobs submitting arrays - include a while loop that waits until all jobs are done before issuing?


## Wait about 5 minutes to let the pre processing complete, and scheduler submission of the array job.
## There are no "futures" in slurm. The job id passed to --dependency must precede the current job id.
# sleep 300

## post-processing compute c5.24xlarge - 96vCPU -> 48 CPU -2 (fim code requirement) = -j 46
if [ $skipPost -eq 0 ] && [ $partitions -eq 0 ]; then
    bash slurm_post_processing.sh ${runName} ${SLURM_PROCESS_UNIT_JOB_ARRAY_ID}
    printf "\n slurm_post_processing.sh submitted, this depends on all of the array jobs completing. \n"
elif [ $skipPost -eq 0 ] && [ $partitions -ne 0 ];then 
    bash slurm_post_processing.sh ${runName} ${SLURM_PROCESS_UNIT_JOB_ARRAY_IDS}
    printf "\n slurm_post_processing.sh submitted, this depends on all of the array jobs completing. \n"
else
    printf "\n slurm_post_processing.sh skipeed, please remember to run the post processing step after all HUCs have completed. \n"
fi

#####################################################################################################################

## Revert runName arg back to placeholder
sed -i -e "s/$runName/placeholder/g" slurm_process_unit_wb.sh 
