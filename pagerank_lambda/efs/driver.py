import json
import boto3
import glob
import os
import subprocess
import lambdautils
import decimal
import time
from botocore.client import Config
from boto3.dynamodb.types import DYNAMODB_CONTEXT
from threading import Thread
import fcntl

os.system('export serverless_mapreduce_role=arn:aws:iam::741926482963:role/biglambda_role')

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

lambda_config = Config(read_timeout=lambda_read_timeout, max_pool_connections=boto_max_connections)
lambda_client = boto3.client('lambda', config=lambda_config)


def write_to_s3(bucket, key, data):
    s3.Bucket(bucket).put_object(Key=key, Body=data)


def zipLambda(fname, zipname):
    subprocess.call(['zip', zipname] + glob.glob(fname + '.py'))


def removeZip(zipname):
    subprocess.call(['rm', '-rf', zipname])


def invoke_lambda(current_iter, end_iter, remain_page, file, pagerank_init):
    '''
    Lambda 함수를 호출(invoke) 합니다.
    '''

    resp = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='Event',
        Payload=json.dumps({
            "current_iter": current_iter,
            "end_iter": end_iter,
            "remain_page": remain_page,
            "file": file,
            "pagerank_init": pagerank_init
        })
    )
    return True


def get_s3_object(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode())


# page들의 관계 데이터셋을 만들어 반환하는 함수 입니다.
def get_page_relation(file):
    return get_s3_object(bucket, config['relationPrefix'] + file)


# zipLambda(lambda_name, lambda_zip)
# l_pagerank = lambdautils.LambdaManager(lambda_client, s3_client, region, config["lambda"]["zip"], lambda_name,
#                                        config["lambda"]["handler"])
# l_pagerank.update_code_or_create_on_noexist()

# page의 관계들이 담겨있는 파일을 가지고 dictionary 관계 데이터셋을 만듭니다.
page_relations = {}
divided_page_num = config["divided_page_num"]
invoked_lambda_num = config["invoked_lambda_num"]

# 전체 페이지의 개수를 계산합니다.
for i in range(invoked_lambda_num + 1):
    print(i)
    try:
        page_relations.update(get_s3_object(bucket, config['relationPrefix'] + str(i) + '.txt'))
    except:
        pass
total_pages = get_s3_object(bucket, config['relationPrefix'] + 'total_page.txt')

total_page_length = len(total_pages)
pagerank_init = 1 / total_page_length
rank_path = '/mnt/efs/ap/' + 'rank_file'
relation_path = '/mnt/efs/ap/' + 'page_relation'

init_return = []
for page in total_pages:
    distance = int(page) * 10
    pagerank_init = str(pagerank_init)
    with open(rank_path, 'r+b', 0) as f:
        # file lock : start_byte 부터 10개의 byte 범위를 lock
        fcntl.lockf(f, fcntl.LOCK_EX, 10, distance, 1)
        for idx in range(10):
            f.seek(distance + idx)
            f.write(pagerank_init[idx].encode())
        print(page)
        # file lock : start_byte 부터 10개의 byte 범위를 unlock
        fcntl.lockf(f, fcntl.LOCK_UN, distance, 1)
        f.close()
    try:
        relation_length = str(len(page_relations[page]))
    except:
        relation_length = str(1)
    with open(relation_path, 'r+b', 0) as f:
        # file lock : start_byte 부터 10개의 byte 범위를 lock
        fcntl.lockf(f, fcntl.LOCK_EX, 10, distance, 1)
        for idx in range(len(relation_length)):
            f.seek(distance + idx)
            f.write(relation_length[idx].encode('utf-8'))
        # file lock : start_byte 부터 10개의 byte 범위를 unlock
        fcntl.lockf(f, fcntl.LOCK_UN, distance, 1)
        f.close()

print('init 끝')
# 모든 page의 초기 Rank값은 1/(전체 페이지 수) 의 값을 가집니다.

# 앞서 zip으로 만든 파일이 Lambda에 업로드 되었으므로 로컬에서의 zip파일을 삭제합니다.
# removeZip(lambda_zip)
# 반복 횟수를 설정합니다.
end_iter = 3
dampen_factor = 0.8
remain_page = (1 - dampen_factor) / total_page_length

print('pages 총 개수:', total_page_length)
print('pages 분할 개수:', divided_page_num)

# S3의 나뉘어진 파일 수 만큼 람다를 병렬적으로 Invoke합니다.
t_return = []
for idx in range(100):
    s3_file_path = config['relationPrefix'] + str(idx) + '.txt'
    print(idx, '번째 invoking')
    t = Thread(target=invoke_lambda,
               args=(1, end_iter, remain_page, s3_file_path, pagerank_init))
    t.start()
    t_return.append(t)
for t in t_return:
    t.join()
#
# for idx in range(invoked_lambda_num + 1):
#     try:
#         s3_file_path = config['relationPrefix'] + str(idx) + '.txt'
#         print(idx, '번째 invoking')
#         invoke_lambda(1, end_iter, remain_page, s3_file_path, pagerank_init)
#     except:
#         pass
