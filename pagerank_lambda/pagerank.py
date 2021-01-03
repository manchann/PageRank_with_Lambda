import boto3
import json
import resource
import time

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def lambda_handler(event, context):
    bucket = event['bucket']
    page = event['page']
    page_relation = event['page_relation']
    file_read_path = str(event['iter']) + '.txt'
    with open(file_read_path, 'r+b', 0) as f:
        f.seek(int(page))
        f.write(page_rank.encode())
