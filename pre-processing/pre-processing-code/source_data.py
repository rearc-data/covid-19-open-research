import os
import csv
import boto3
from urllib.request import urlopen

def source_dataset(s3_bucket, new_s3_key):

    source_dataset_url = 'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/metadata.csv'

    source_csv = urlopen(source_dataset_url).read().decode().splitlines()

    with open('/tmp/metadata.csv', 'w', encoding='utf-8') as c:

        c.write(source_csv[0].lower().replace(
            ' ', '_').replace('#', '') + '\n')

        for row in source_csv[1:]:
            c.write(row + '\n')

    # uploading new s3 dataset
    filename = 'metedata.csv'

    s3 = boto3.client('s3')
    s3.upload_file('/tmp/' + filename, s3_bucket, new_s3_key + filename)