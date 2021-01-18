import json
import boto3
import glob
import subprocess
import lambdautils
import decimal
import time
from botocore.client import Config
from boto3.dynamodb.types import DYNAMODB_CONTEXT

# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
db_name = 'jg-pagerank'
table = dynamodb.Table(db_name)

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


def write_to_s3(bucket, key, data):
    s3.Bucket(bucket).put_object(Key=key, Body=data)


def zipLambda(fname, zipname):
    subprocess.call(['zip', zipname] + glob.glob(fname + '.py'))


def removeZip(zipname):
    subprocess.call(['rm', '-rf', zipname])


def invoke_lambda(page, page_relations, iter, remain_page):
    '''
    Lambda 함수를 호출(invoke) 합니다.
    '''

    resp = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "page": page,
            "page_relation": page_relations,
            "iter": iter,
            "remain_page": remain_page
        })
    )


# page들의 관계 데이터셋을 만들어 반환하는 함수 입니다.
def get_page_relation(pages):
    page_relations = {}
    pages = pages['Body'].read().decode()
    lines = pages.split("\n")
    for line in lines:
        key = line.split("\t")[0]
        value = line.split("\t")[1]
        if key not in page_relations:
            page_relations[key] = []
        if value not in page_relations[key]:
            page_relations[key].append(value)
    return page_relations


def dynamodb_remove_all_items():
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={
                'iter': each['iter'],
                'page': each['page']
            })


# DynamoDB에 있는 모든 값을 지웁니다.
dynamodb_remove_all_items()

zipLambda(lambda_name, lambda_zip)
l_pagerank = lambdautils.LambdaManager(lambda_client, s3_client, region, config["lambda"]["zip"], lambda_name,
                                       config["lambda"]["handler"])
l_pagerank.update_code_or_create_on_noexist()

# page의 관계들이 담겨있는 파일을 가지고 dictionary 관계 데이터셋을 만듭니다.
page_relations = get_page_relation(pages)

print(page_relations)

# 모든 page의 초기 Rank값은 1/전체 페이지 수 의 값을 가집니다.
pagerank_init = 1 / len(page_relations)

# DynamoDB에 모든 페이지의 초기값들을 업로드 합니다.
for page in page_relations:
    table.put_item(
        Item={
            'iter': 0,
            'page': page,
            'rank': decimal.Decimal(str(pagerank_init)),
            'relation': page_relations[page]
        }
    )

# 앞서 zip으로 만든 파일이 Lambda에 업로드 되었으므로 로컬에서의 zip파일을 삭제합니다.
removeZip(lambda_zip)
# case: S3
# file_read_path = '0.txt'
# byte = 1024
# with open(file_read_path, 'w+b', 0) as f:
#     for page in page_relations:
#         f.seek(int(page) * byte)
#         f.write(str(pagerank_init).encode())
# write_to_s3(bucket, file_read_path, open('./0.txt', 'rb'))
# progress = '/progress'
#
# for iter in range(1, iters + 1):
#     for page in page_relations:
#         invoke_lambda(page, page_relations[page], iter)
#         break

# 반복 횟수를 설정합니다.
iters = 25
dampen_factor = 0.8
remain_page = (1 - dampen_factor) / len(page_relations)
# case DynamodbDB
for iter in range(1, iters + 1):
    for page in page_relations:
        invoke_lambda(page, page, iter, remain_page)
    print('%s 번째 진행 중...' % str(iter))
    time.sleep(10)
