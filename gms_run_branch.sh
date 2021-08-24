#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric at levelpath/branch scale. Execute gms_run_unit.sh prior to.'
    echo 'Usage : gms_run.sh [REQ: -u <hucs> -c <config file> -n <run name> ] [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC 4,6,or 8 to run or multiple passed in quotes. Line delimited file'
    echo '                     also accepted. HUCs must present in inputs directory.'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -p/--production : only save final inundation outputs'
    echo '  -w/--whitelist  : list of files to save in a production run in addition to final inundation outputs'
    echo '                     ex: file1.tif,file2.json,file3.csv'
    echo '  -v/--viz        : compute post-processing on outputs to be used in viz'
    exit
}

if [ "$#" -lt 6 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -u|--hucList)
        shift
        hucList="$1"
        ;;
    -c|--configFile )
        shift
        envFile=$1
        ;;
    -n|--runName)
        shift
        runName=$1
        ;;
    -j|--jobLimit)
        shift
        jobLimit=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    -o|--overwrite)
        overwrite=1
        ;;
    -p|--production)
        production=1
        ;;
    -w|--whitelist)
        shift
        whitelist="$1"
        ;;
    -v|--viz)
        viz=1
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$hucList" = "" ]
then
    usage
fi
if [ "$envFile" = "" ]
then
    usage
fi
if [ "$runName" = "" ]
then
    usage
fi
if [ "$overwrite" = "" ]
then
    overwrite=0
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $srcDir/bash_functions.env

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$default_max_jobs
fi

## Define Outputs Data Dir & Log File##
export outputRunDataDir=$outputDataDir/$runName
export production=$production
export whitelist=$whitelist
export viz=$viz
logFile=$outputRunDataDir/logs/summary_gms_branch.log
export extent=GMS

## Make output and data directories ##
if [ -d "$outputRunDataDir" ]; then 
    branch_directories_count=$(find $outputRunDataDir/**/gms/ -maxdepth 1 -mindepth 1 -type d | wc -l)

    if [ $branch_directories_count -gt 0 ] && [ "$overwrite" -eq 1 ]; then
        rm -rf $branch_directories_list
    elif [ $branch_directories_count -gt 0 ] && [ "$overwrite" -eq 0 ] ; then
        echo "GMS branch data directories for $runName already exist. Use -o/--overwrite to continue"
        exit 1
    fi
else
    echo "GMS depends on Full Resolution Data. Please produce data with fim_run.sh first."
    exit 1
fi

# make log dir
mkdir -p $outputRunDataDir/logs


## RUN GMS BY BRANCH ##
if [ "$jobLimit" -eq 1 ]; then
    parallel --verbose --timeout $branch_timeout --lb  -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $outputRunDataDir/gms_inputs.csv
else
    parallel --eta --timeout $branch_timeout -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $outputRunDataDir/gms_inputs.csv
fi

