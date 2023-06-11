#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse

import azure.batch

from azure.batch import models
from azure.batch.batch_auth import SharedKeyCredentials

import configs

if __name__ == "__main__" :


    parser = argparse.ArgumentParser(
                                    description='Destroy one or many jobs of Batch account.',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                    )
    parser.add_argument('--job_ids', help="List of job id('job_1,job_2')", dest='job_ids')
    args = parser.parse_args()


    job_ids = args.job_ids


    credentials_batch = SharedKeyCredentials(account_name = configs.batch['name'], key = configs.batch['key'])
    batch_client = azure.batch.BatchServiceClient(credentials = credentials_batch, batch_url = configs.batch['url'])


    for job_id in job_ids.strip().replace(' ','').split(','):
        batch_client.job.delete(job_id)
        print("Destroying of job: {0}.".format(job_id))