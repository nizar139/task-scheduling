
                                                    ##########################
                                                    #  Tutorial Azure Batch  #
                                                    ##########################

I. File list
------------

#check_task.py            Checking status for a job of tasks and downloads the stderr/stdout outputs if all tasks are finished.
config_resources.py      Containing information about the batch account and the blob container used.
create_pool_job.py       Creating a pool of compute nodes and Job(future tasks sent to a job will transmit to pool)
demo_azure_batch/        Contains all the scripts downloaded by a node of the pool.
send_task.py             Sending a task to a job





II - Description files
----------------------


#  A) check_task.py

#1) Login to an Azure batch account (from the information in config_resources.py)
#2) Displaying tasks status summary of each job in the batch account
#3) If all tasks are finished within a job, the output files (stdout, stderr) will be downloaded



  B) config_resources.py

File containing the following variables:

- resource_gpe_name: contains the name of a resource group

- batch: contains the name, primary key and url of an Azure batch account

- blob_container: contains the url and the token SAS of a Azure blob container

- repository: Repo path containing inputs files needs to task

- cmd_prep_task: Contains the set of commands that will be executed on each node before running the main task
  EXAMPLE: cmd_prep_task = (
              "bash -c 'git clone {0} ; cd demo_azure_batch ; chmod +x install.sh; ./install.sh'".format(repository)
              )

- nb_processes: Nb processes MPI executed

- coordination_command: contains the set of commands to prepare nodes to process inter-node messages

- start_command: Main command executed by task



  C) create_pool_job.py

1) Login to an Azure batch account (from the information in config_resources.py)

2) Creating of a pool (image: UbuntuServer 18.04-LTS)
    - the inter-node communication is activated
    - size pool increases (max 5 nodes) and decreases according to the number of tasks via the definition of an autoscaling rule

3) Creating a job
    - added a preparation task that clones a repo git (repo content == content of demo_azure_batch folder) and runs an installation script inside it



  D) send_task.py

1) Creating an mpi task
   - When the task is finished, the output is stored in the blob container of the Azure storage account.



  E) demo_azure_batch/

This folder contains 3 files:
- install.sh (installs all the packages needed to perform the task)
- requirements_node.txt (list of python packages needed to run the task)
- script.py (program executed by the task)





III - Prerequisites
-------------------



A) Install python 3.7 and packages needed

sudo apt install python3-pip -y
pip install azure-batch


B) Modify variables (cmd_prep_task, repository, coordination_command, start_command) in configs.py according to use cases




IV - Create a pool of compute node and a job.py
-----------------------------------------------

command: python3 create_pool_job.py --pool_id POOL_ID --job_id JOB_ID




V - Send a mpi task to a job
----------------------------

command: python3 send_task.py --job_id JOB_ID --task_id TASK_ID




VI (Optional) - Check status tasks
---------------------------------

command: python3 check_task.py




