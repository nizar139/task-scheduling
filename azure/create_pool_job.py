#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse

import azure.batch

from azure.batch import models
from azure.batch.batch_auth import SharedKeyCredentials

import configs



def create_pool(batch_client, name_pool, cmd_s_task=None, rule_scale_pool=None):

    #parameter image node
    param_image = models.VirtualMachineConfiguration(
        image_reference = models.ImageReference(
        offer = 'UbuntuServer',
        publisher = 'Canonical',
        sku = '18.04-LTS',
        version = 'latest',
        virtual_machine_image_id = None
        ) ,
      node_agent_sku_id = 'batch.node.ubuntu 18.04'
    )

    #parameter pool
    new_pool = models.PoolAddParameter(
        id = name_pool, 
        vm_size = 'standard_d1_v2',
        virtual_machine_configuration = param_image,
        enable_inter_node_communication = True,
        enable_auto_scale = True,
        auto_scale_formula = rule_scale_pool,
        auto_scale_evaluation_interval = 'PT5M'
        )
    batch_client.pool.add(new_pool)



def create_job(batch_client, name_job, name_pool, cmd_prep_task=None):

    user = models.UserIdentity(
    auto_user = models.AutoUserSpecification(
        elevation_level = models.ElevationLevel.admin,
        scope = models.AutoUserScope.task
        )
    )

    prepare_task = models.JobPreparationTask(
        command_line = cmd_prep_task,
        id = None,
        user_identity = user
        )

    job = models.JobAddParameter(
        id = name_job,
        pool_info = models.PoolInformation(pool_id = name_pool),
        job_preparation_task = prepare_task
        )
    batch_client.job.add(job)








if __name__ == "__main__" :


    parser = argparse.ArgumentParser(
                                    description='Creating a pool of compute nodes and Job(future tasks sent to a job will transmit to pool).',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                    )
    parser.add_argument('--pool_id', help='Id of pool', dest='pool_id', default='CentraleSupelecPool1')
    parser.add_argument('--job_id', help='Id of job', dest='job_id', default='CentraleSupelecJob1')
    args = parser.parse_args()


    pool_id = args.pool_id
    job_id = args.job_id


    credentials_batch = SharedKeyCredentials(account_name = configs.batch['name'], key = configs.batch['key'])
    batch_client = azure.batch.BatchServiceClient(credentials = credentials_batch, batch_url = configs.batch['url'])



    create_pool(
                batch_client = batch_client,
                name_pool = pool_id,
                rule_scale_pool = configs.rule_scaling
               )
    


    create_job(
               batch_client = batch_client,
               name_job = job_id,
               name_pool = pool_id, 
               cmd_prep_task = configs.cmd_prep_task
              )


    print("Pool Created: {0}\nJob created: {1}\n".format(pool_id,job_id))
    with open('output_pool_id_job_id.txt','w') as file_resource:
        file_resource.write("Pool Created: {0}\nJob created: {1}\n".format(pool_id,job_id))