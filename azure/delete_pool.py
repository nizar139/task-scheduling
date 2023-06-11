#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse

import azure.batch

from azure.batch import models
from azure.batch.batch_auth import SharedKeyCredentials

import configs

if __name__ == "__main__" :


    parser = argparse.ArgumentParser(
                                    description='Destroy one or many pools of Batch account.',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                    )
    parser.add_argument('--pool_ids', help="List of pool id('pool_1,pool_2')", dest='pool_ids')
    args = parser.parse_args()


    pool_ids = args.pool_ids


    credentials_batch = SharedKeyCredentials(account_name = configs.batch['name'], key = configs.batch['key'])
    batch_client = azure.batch.BatchServiceClient(credentials = credentials_batch, batch_url = configs.batch['url'])


    for pool_id in pool_ids.strip().replace(' ','').split(','):
        batch_client.pool.delete(pool_id)
        print("Destroying of pool: {0}.".format(pool_id))