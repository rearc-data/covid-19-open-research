import os
import pandas as pd
import boto3
import botocore

# function to return most recent date str
def compare_dates(latest, new):

    latest_int = [int(sub_date) for sub_date in latest.split('-')]
    new_int = [int(sub_date) for sub_date in new.split('-')]

    if latest_int[0] != new_int[0]:
        return latest if latest_int[0] > new_int[0] else new
    elif latest_int[1] != new_int[1]:
        return latest if latest_int[1] > new_int[1] else new
    else:
        return latest if latest_int[2] > new_int[2] else new

def source_dataset(s3_bucket, new_s3_key):

    # variables to construct the file's location
    source_dataset_url = 'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/'
    bucket_name = 'ai2-semanticscholar-cord-19'
    file_name = 'metadata.csv'
    date = '2020-03-20'

    # accessing the public bucket were the desired file is located
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket_name)
    exists = True
    try:
        resource.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            exists = False

    # interating through the objects in the bucket, and updating the
    # date variable if a more recent date is found
    for key in bucket.objects.filter(Prefix='20'):
        key_split = key.key.split('/', 1)
        if key_split[1] == file_name:
            date = compare_dates(key_split[0], date)

    df = pd.read_csv(source_dataset_url + date + '/' + file_name,
                     header=0, dtype={'WHO #Covidence': 'str'}, index_col=None)
                     
    # converts columns names to follow sql best practices and consistently
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('#', '')
    df.columns = df.columns.str.lower()

    # convert all data to lower case for any data type equals string
    df = df.applymap(lambda s: s.lower() if type(s) == str else s)

    df.to_csv('/tmp/' + file_name, index=False)

    # # uploading new s3 dataset
    s3 = boto3.client('s3')
    s3.upload_file('/tmp/' + file_name, s3_bucket, new_s3_key)