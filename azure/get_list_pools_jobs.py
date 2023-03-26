#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse

import azure.batch

from azure.batch import models
from azure.batch.batch_auth import SharedKeyCredentials

import configs


credentials_batch = SharedKeyCredentials(account_name = configs.batch['name'], key = configs.batch['key'])
batch_client = azure.batch.BatchServiceClient(credentials = credentials_batch, batch_url = configs.batch['url'])


print("List of pools from batch account: {0}".format(configs.batch['name']))
for pool in batch_client.pool.list():
    print(pool.id)

print("\nList of jobs from batch account: {0}".format(configs.batch['name']))
for job in batch_client.job.list():
    print(job.id)