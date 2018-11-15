#!/usr/bin/env python3.6
# Author: Edouard Bardet, CTO, SMALLable
# License: GPL

import argparse
import logging
import time
import boto3
import os

# Google service account authentication is tricky.
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.getLogger('googleapiclient.discovery').setLevel(logging.CRITICAL)

parser          = argparse.ArgumentParser()
parser.add_argument('--project_id', help='BigQuery project ID.', default='sml-bi')
parser.add_argument('--dataset_id', help='BigQuery dataset ID.', default='smlbi')
parser.add_argument('--table_name', help='Name of the table to load csv file into.', default='sales4')
parser.add_argument('--file_name', help='Name of the file to load.', default=False)

args            = parser.parse_args()
client          = bigquery.Client.from_service_account_json('./auth.json')


try:
    dataset_id  = os.environ['dataset_id']
except KeyError:
    dataset_id  = args.dataset_id

try:
    table_name  = os.environ['table_name']
except KeyError:
    table_name  = args.table_name
try:
    project_id  = os.environ['project_id']
except KeyError:
    project_id  = args.project_id    

dataset         = client.dataset(dataset_id)
table           = dataset.table(table_name)
dest_table      = table_name
message         = 'Nothing loaded'


def field_to_name(field):
    return field[0]

def field_to_cast(field):
    #BQ api do not return same data type
    data_type = field[1]
    if field[1] == 'INTEGER':
        data_type = 'INT64'
    if field[1] == 'FLOAT':
        data_type = 'FLOAT64'        
    return 'Cast(' + field[0] + ' As ' + data_type + ')'

def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)

def field_to_update(field):
    source_field = ['source.'+ field[0],field[1]]
    return field[0]+' = '+field_to_cast(source_field) 

try:
    table_data   = client.get_table(table)
    table_schema = table_data.schema
    table_exists = True
    table_ref    = table_name+'_'+time.strftime('%Y%m%d%H%M%S')
    source_table = table_ref
except NotFound:
    table_exists = False
    table_ref    = table_name
    source_table = table_ref

if table_exists == True:
    fields       = []
    primary_key  = False
    for field in table_schema:
        fields.append([field.name,field.field_type])
        if field.description == 'primary_key':
            primary_key = field.name
else:
    primary_key  = True


if primary_key == False:
    print('Primary key must be set as description (primary_key) at least on one field in the destination table.')
    exit()


def push2gbg(filename):
    global message
    table_name = dataset.table(table_ref)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.null_marker = 'NULL'
    job_config.max_bad_records = 10

    with open(filename, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_name,
            job_config=job_config)  # API request

    wait_for_job(job)  # Waits for table load to complete.

    message     = 'Loaded {} rows into {}:{}.'.format(
                    job.output_rows, args.dataset_id, table_name)

    destination = project_id+'.'+dataset_id+'.'+dest_table
    source      = project_id+'.'+dataset_id+'.'+source_table

    if table_exists == True:
        QUERY = """
            #standardSQL
            MERGE `%s` destination
            USING `%s` source
            ON destination.%s = source.%s
            WHEN MATCHED THEN
                UPDATE SET %s
            WHEN NOT MATCHED THEN 
                INSERT(%s) Values(%s)
            """ % (destination
                    ,source
                    ,primary_key
                    ,primary_key
                    , ','.join(map(field_to_update, fields))
                    , ','.join(map(field_to_name, fields))
                    , ','.join(map(field_to_cast, fields))
                    )
        #print(QUERY)

        #QUERY = """
        #    #standardSQL
        #    Insert Into `%s` (%s)
        #    Select %s 
        #    From `%s` src
        #    Where Not Exists(Select 1 From `%s` dest Where src.%s = dest.%s)""" % ('sml-bi.'+dataset_id+'.'+table_name, ','.join(map(field_to_name, fields)), ','.join(map(field_to_cast, fields)), 'sml-bi.'+dataset_id+'.'+table_ref, dataset_id+'.'+source_table,primary_key,primary_key)
        
        query_job = client.query(QUERY)
        client.delete_table(dataset.table(table_ref))


if args.file_name != False:
    push2gbg(args.file_name)
    print(message)


def handler(event, context):
    for record in event['Records']:
        bucket  = record['s3']['bucket']['name']
        key     = record['s3']['object']['key']
        s3      = boto3.resource('s3')
        obj     = s3.Object(bucket, key).download_file('/tmp/temp')
        push2gbg('/tmp/temp')
        print(message)
        return { 
        'message' : message
        }
