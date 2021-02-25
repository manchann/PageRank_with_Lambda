import boto3
import json
import resource
import time
import decimal
from botocore.client import Config
from threading import Thread
import fcntl

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
db_name = 'jg-pagerank'
rank_table = dynamodb.Table(db_name)

lambda_read_timeout = 300
boto_max_connections = 1000
lambda_config = Config(read_timeout=lambda_read_timeout, max_pool_connections=boto_max_connections)
lambda_client = boto3.client('lambda', config=lambda_config)
lambda_name = 'pagerank'
bucket = "jg-pagerank-bucket2"
rank_path = '/mnt/efs/' + 'rank_file'


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key):
    s3.Bucket(bucket).put_object(Key=key)


def get_s3_object(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode())


def invoke_lambda(current_iter, end_iter, remain_page, file):
    '''
    Lambda 함수를 호출(invoke) 합니다.
    '''

    resp = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "current_iter": current_iter,
            "end_iter": end_iter,
            "remain_page": remain_page,
            "file": file,
        })
    )


def get_past_pagerank(page):
    page = int(page)
    rank = ''
    with open(rank_path, 'r+b', 0) as f:
        # file lock : start_byte 부터 10개의 byte 범위를 lock
        fcntl.lockf(f, fcntl.LOCK_EX, 10, page, 1)
        for idx in range(10):
            rank += f.read(page * (idx + 1))
        # file lock : start_byte 부터 10개의 byte 범위를 unlock
        fcntl.lockf(f, fcntl.LOCK_UN, page, 1)
        f.close()
    print('get', rank)
    return rank


def put_efs(page, rank):
    page = int(page)
    with open(rank_path, 'r+b', 0) as f:
        # file lock : start_byte 부터 10개의 byte 범위를 lock
        fcntl.lockf(f, fcntl.LOCK_EX, 10, page, 1)
        for idx in range(10):
            f.seek(page * (idx + 1))
            f.write(rank[idx].encode())
        # file lock : start_byte 부터 10개의 byte 범위를 unlock
        fcntl.lockf(f, fcntl.LOCK_UN, page, 1)
        f.close()
    print('put', rank)
    return rank


dampen_factor = 0.8


# 랭크를 계산합니다.
def ranking(page_relation):
    rank = 0
    get_time = 0
    for page in page_relation:
        # dynamodb에 올려져 있는 해당 페이지의 rank를 가져옵니다.
        get_start = time.time()
        past_rank = get_past_pagerank(page)
        get_time += time.time() - get_start
        # rank += float(past_info['rank']) / float(past_info['relation_length'])
        rank += float(past_rank)
    rank *= dampen_factor
    return rank, get_time


# 각각 페이지에 대하여 rank를 계산하고 dynamodb에 업데이트 합니다.
def ranking_each_page(page, page_relation, iter, remain_page):
    rank_start = time.time()
    rank, get_time = ranking(page_relation)
    page_rank = rank + remain_page
    rank_time = time.time() - rank_start
    put_start = time.time()
    put_efs(page, page_rank)
    put_time = time.time() - put_start
    return {'iter': iter, 'page': page, 'get_time': get_time, 'rank_time': rank_time, 'put_time': put_time,
            'page_rank': page_rank}


def lambda_handler(event, context):
    current_iter = event['current_iter']
    end_iter = event['end_iter']
    remain_page = event['remain_page']
    file = event['file']
    page_relations = get_s3_object(bucket, file)
    try:
        for page, page_relation in page_relations.items():
            ranking_result = ranking_each_page(page, page_relation, current_iter, remain_page)
            print(ranking_result)
        # current_iter = end_iter이 되기 전 까지 다음 iteration 람다를 invoke합니다.
        if current_iter < end_iter:
            invoke_lambda(current_iter + 1, end_iter, remain_page, file)
    except:
        print('error')
        return True
    return True
