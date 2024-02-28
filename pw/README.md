# FIM HAND Dataset Generation on PW 
### Intro:
Efforts are underway to architect and parallelize the HUC8 level processing using a traditional HPC workflow (Slurm Scheduler to many compute nodes) to maximize resources available, as well as decrease the time to solution.

There are a few pieces that are necessary to have in place for the whole system to run smoothly. In terms of seeding the necessary input data in a Filesystem, this is dependent on the Cloud Service Provider being used. 


## Prerequisite PW resources:

An EFS provisioned and accessible. (eg: fimtest or fimefs)
    The EFS is used for the output path - where the FIM HAND Data will be copied to after computations are completed.

Ephemeral Storage (Optional):

	An AWS FSX for Lustre provisioned and accessible. (eg: fsx)
	
    FSX can be set up as an ephemeral filesystem that may provide a faster platorm for the computations, but will not be used to persist the generated data. The FSX can be configured to mount an S3 Bucket. The S3 URI should match what was output as the bucket_name = “”, when provisioning the S3 bucket in the PW platform (not what is visible as the name in the PW UI.)

## PW Cluster Configuration:
Refer to auxillary documentation for cluster configuration, as this will change based on PW account (Test, Optimization) being used, as well as the Cloud Service Provider. 

## Start the Cluster:
Find your desired cluster under the HOME tab, and My Compute Resources.
Click the power button.
Please note that the power button is essentially a Terraform apply or Terraform destroy, depending on the state. For those not familiar with terraform, that means the resources is provisioned and de-provisioned, so all files and folders (not in the EFS drive) will not persist.

Then you can click the <username>@<ip of cluster> to copy it to your clipboard:

## Connecting to the Cluster Controller Node:

From there, click on the terminal icon at the very top right of the webpage:

You now have a Terminal window that is connected to the PW “User Container” (not the cluster).

Now type `ssh`, and paste the contents of your clipboard from Start the Cluster step above:

From the Controller node, you can ensure that the mounts were successful:

```bash
ls /efs
df -h 
```

## Parallel Processing using schedule from Head Node

Here is an example of calling slurm_pipeline.sh.

```bash
bash slurm_pipeline.sh /data/inputs/huc_lists/dev_small_test_4_huc.lst test_pipeline_wrapper
```

Please see the comments in this script, as well as the other `.sh` files to gain awareness of the procedue used. 

## Serial Processing using interactive compute node

### Connecting to the Cluster Compute Node:
The Controller node would traditionally kick of Slurm Jobs, but in the case of just running on one Compute Node, we need to spin up an interactive Compute Node to issue the script (run on a docker container) that will generate the datasets.

Allocate an interactive compute node
```
salloc --cpus-per-task=1
```

Get JOB ID
```
squeque
```

Collect the Job Id from squeue output (replace <JOB_ID> below) & connect to compute node
```
srun --pty --jobid <JOB_ID> /bin/bash
```

You can verify these two steps were done correctly in two ways: 

    1.) The first will be the most obvious. You should have an updated terminal prompt with @compute included.

    2.) The second is from viewing the amount of active nodes as viewed from the HOME tab in PW (unclick terminal Icon).

