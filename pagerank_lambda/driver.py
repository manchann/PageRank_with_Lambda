import json
import boto3
import glob
import subprocess
from botocore.client import Config

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

config = json.loads(open('driverconfig.json', 'r').read())

bucket = config["bucket"]
region = config["region"]
lambda_memory = config["lambdaMemory"]
concurrent_lambdas = config["concurrentLambdas"]  # 동시 실행 가능 수
lambda_read_timeout = config["lambda_read_timeout"]
boto_max_connections = config["boto_max_connections"]
lambda_name = config["lambda"]["name"]
lambda_zip = config["lambda"]["zip"]
pages = s3_client.get_object(Bucket=bucket, Key=config["pages"])

lambda_config = Config(read_timeout=lambda_read_timeout, max_pool_connections=boto_max_connections)
lambda_client = boto3.client('lambda', config=lambda_config)

iters = 50


def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def zipLambda(fname, zipname):
    subprocess.call(['zip', zipname] + glob.glob(fname))


def invoke_lambda(page, page_relations, iter):
    '''
    Lambda 함수를 호출(invoke) 합니다.
    '''

    resp = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "bucket": bucket,
            "page": page,
            "page_relation": page_relations,
            "iter": iter
        })
    )


def get_page_relation(pages):
    page_relations = {}
    with open(pages, 'r') as f:
        lines = f.readlines()
        for line in lines:
            key = line.split(" ")[0]
            value = line.split(" ")[1]
            if key not in page_relations:
                page_relations[key] = []
            page_relations[key].append(value)
    return page_relations


zipLambda(lambda_name, lambda_zip)

page_relations = get_page_relation(pages)

print(page_relations)

file_read_path = '0.txt'
pagerank_init = 1 / len(page_relations)
with open(file_read_path, 'r+b', 0) as f:
    for page in page_relations:
        f.seek(int(page))
        f.write(str(pagerank_init).encode())

for iter in range(1, iters + 1):
    for page in page_relations:
        invoke_lambda(page, page_relations[page], iter)
        break
