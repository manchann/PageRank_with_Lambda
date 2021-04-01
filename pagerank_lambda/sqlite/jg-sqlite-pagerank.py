import boto3
import json
import resource
import time
import decimal
from botocore.client import Config
from threading import Thread
import fcntl
import sqlite3
import os

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

lambda_read_timeout = 300
boto_max_connections = 1000
lambda_config = Config(read_timeout=lambda_read_timeout, max_pool_connections=boto_max_connections,
                       retries={'max_attempts': 0})
lambda_client = boto3.client('lambda', region_name='us-west-2', config=lambda_config)
lambda_name = 'jg-sqlite-pagerank'
bucket = "jg-pagerank-bucket2"

total_divide_num = 4840

db_path = "/mnt/efs/"


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
        InvocationType='Event',
        Payload=json.dumps({
            "current_iter": current_iter,
            "end_iter": end_iter,
            "remain_page": remain_page,
            "file": file,
        })
    )
    return True


reader_arr = {}


def get_past_pagerank(get_query_arr):
    ret = []
    for idx in range(len(get_query_arr)):
        if get_query_arr[idx] == '0':
            continue
        get_query_arr[idx] = get_query_arr[idx][:len(get_query_arr[idx]) - 4] + ';'

        dict_idx = str(idx)
        if dict_idx not in reader_arr:
            reader_arr[dict_idx] = sqlite3.connect(db_path + dict_idx + '.db', timeout=900)
        reader = reader_arr[dict_idx]
        cur = reader.cursor()
        cur.execute(get_query_arr[idx])
        res = cur.fetchall()
        ret += res
    return ret


def put_efs(data, writer):
    cur = writer.cursor()
    cur.executemany('REPLACE INTO pagerank VALUES (?, ?, ?, ?);', data)
    writer.commit()
    return True


dampen_factor = 0.8


# 랭크를 계산합니다.
def ranking(page_relation):
    rank = 0

    get_query_arr = ['0' for i in range(total_divide_num + 1)]
    page_query = "SELECT * FROM pagerank Where "
    for page in page_relation:
        # dynamodb에 올려져 있는 해당 페이지의 rank를 가져옵니다.
        db_num = int(page) // 1000
        if get_query_arr[db_num] == '0':
            get_query_arr[db_num] = page_query
        get_query_arr[db_num] += "page=" + page + " OR "
    get_start = time.time()
    past_pagerank = get_past_pagerank(get_query_arr)
    get_time = time.time() - get_start

    for page_data in past_pagerank:
        past_rank = page_data[2]
        relation_length = page_data[3]
        rank += (past_rank / relation_length)
    rank *= dampen_factor
    return rank, get_time


# 각각 페이지에 대하여 rank를 계산하고 dynamodb에 업데이트 합니다.
def ranking_each_page(page, page_relation, iter, remain_page):
    rank_start = time.time()
    rank, get_time = ranking(page_relation)
    page_rank = rank + remain_page
    rank_time = time.time() - rank_start

    return {'iter': iter,
            'page': page,
            'get_time': get_time,
            'rank_time': rank_time,
            'page_rank': page_rank,
            'relation_length': len(page_relation)}


def lambda_handler(event, context):
    start = time.time()
    current_iter = event['current_iter']
    end_iter = event['end_iter']
    remain_page = event['remain_page']
    file = event['file']
    page_relations = get_s3_object(bucket, file)

    db_name = file.split('/')[2]
    db_name = int(db_name.split('.')[0])
    writer = sqlite3.connect(db_path + str(db_name) + '.db', timeout=900)
    # cur = writer.cursor()
    # cur.execute('pragma journal_mode = DELETE;')
    # cur.execute('pragma busy_timeout = 600;')
    while current_iter <= end_iter:
        iter_start = time.time()
        ret = []
        for page, page_relation in page_relations.items():
            ranking_result = ranking_each_page(page, page_relation, current_iter, remain_page)
            result = (ranking_result['page'], ranking_result['iter'], ranking_result['page_rank'],
                      ranking_result['relation_length'])
            ret.append(result)
            print(ranking_result)
        put_start = time.time()
        put_efs(ret, writer)
        put_time = time.time() - put_start
        print({'put_time': put_time,
               'iter': current_iter,
               'file': file
               })
        print(str(current_iter) + ' 번째 iteration 걸린 시간: ', time.time() - iter_start)
        current_iter += 1

    print('총 걸린 시간:', str(file) + ' 번 람다', str(end_iter) + " 번 작업", time.time() - start)
    return True
