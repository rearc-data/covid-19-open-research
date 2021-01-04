import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import time

def source_dataset(s3_bucket, new_s3_key):

	source_dataset_url = 'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/metadata.csv'

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	data = None

	retries = 5
	for attempt in range(retries):
		try:
			data = urlopen(source_dataset_url)
		except HTTPError as e:
			if attempt == retries:
				raise Exception('HTTPError: ', e.code, source_dataset_url)
			time.sleep(0.2 * attempt)

		except URLError as e:
			if attempt == retries:
				raise Exception('URLError: ', e.reason, source_dataset_url)
			time.sleep(0.2 * attempt)
		else:
			break

	data = data.read().decode().splitlines()
	data[0] = data[0].lower().replace(' ', '_').replace('#', '')
	data = '\n'.join(data)
	data = data.encode()

	# uploading new s3 dataset
	s3 = boto3.client('s3')
	s3.put_object(Body=data, Bucket=s3_bucket, Key=new_s3_key)
	
	# deletes to preserve limited space in aws lamdba
	data = None

	return [{'Bucket': s3_bucket, 'Key': new_s3_key}]
