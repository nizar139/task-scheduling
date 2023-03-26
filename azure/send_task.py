#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import argparse

from time import localtime

import azure.batch

from azure.batch import models
from azure.batch.batch_auth import SharedKeyCredentials

import configs



def create_task(batch_client, name_job, cmd, name_task, param_multi_inst=None):

    current_date = localtime()[0:5]
    current_date = "{0}{1}{2}{3}{4}".format(
                                            current_date[0],current_date[1],
                                            current_date[2],current_date[3],
                                            current_date[4]
                                           )

    dest_files_in_container = models.OutputFileBlobContainerDestination(
        container_url = f"{configs.blob_container['url']}{configs.blob_container['sas_token']}",
        path = f"{name_job}/{name_task}/{current_date}"
        )

    dest_files = models.OutputFileDestination(container = dest_files_in_container )

    trigger_upload = models.OutputFileUploadOptions(upload_condition = 'taskCompletion')

    upload_files = models.OutputFile(
                                     file_pattern = '$AZ_BATCH_TASK_DIR/*' ,
                                     destination = dest_files,
                                     upload_options = trigger_upload
                                    )
    outputs = []
    outputs.append(upload_files)
    tache = models.TaskAddParameter(
        id = name_task, command_line = cmd,
        multi_instance_settings = param_multi_inst,
        resource_files = None, environment_settings = None,
        output_files = outputs
        )
    batch_client.task.add(name_job,tache)







if __name__ == "__main__" :


    parser = argparse.ArgumentParser(
                                    description='Sending a task to a job.',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                    )
    parser.add_argument('--job_id', help='Id of job', dest='job_id')
    parser.add_argument('--task_id', help='Id of task', dest='task_id', default='task1')
    args = parser.parse_args()


    job_id = args.job_id
    task_id = args.task_id


    credentials_batch = SharedKeyCredentials(account_name = configs.batch['name'], key = configs.batch['key'])
    batch_client = azure.batch.BatchServiceClient(credentials = credentials_batch, batch_url = configs.batch['url'])


    #copier le script d'execution dans le dossier partagé du noeud
    multi_instance_task_param = models.MultiInstanceSettings(
                         coordination_command_line = configs.coordination_command ,
                         number_of_instances = min(configs.nb_processes,5),
                         common_resource_files = None
                        )
    

    create_task(
                batch_client = batch_client,
                name_job = job_id,
                cmd = configs.start_command,
                name_task = task_id,
                param_multi_inst = multi_instance_task_param
               )

