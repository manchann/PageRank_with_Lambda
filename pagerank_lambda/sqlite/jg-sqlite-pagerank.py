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

db_path = '/mnt/efs/'


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


def get_past_pagerank(get_query_arr, reader_arr):
    ret = []
    for idx in range(len(get_query_arr)):
        if get_query_arr[idx] == '0':
            continue
        get_query_arr[idx] = get_query_arr[idx][:len(get_query_arr[idx]) - 4] + ';'
        reader = sqlite3.connect(db_path + str(idx) + '.db')
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
def ranking(page_relation, reader_arr):
    rank = 0

    get_query_arr = ['0' for i in range(total_divide_num + 1)]
    page_query = 'SELECT * FROM pagerank Where '
    for page in page_relation:
        # dynamodb에 올려져 있는 해당 페이지의 rank를 가져옵니다.
        db_num = int(page) // 1000
        if get_query_arr[db_num] == '0':
            get_query_arr[db_num] = page_query
        get_query_arr[db_num] += 'page=' + page + ' OR '
    get_start = time.time()
    past_pagerank = get_past_pagerank(get_query_arr, reader_arr)
    get_time = time.time() - get_start

    for page_data in past_pagerank:
        past_rank = page_data[2]
        relation_length = page_data[3]
        rank += (past_rank / relation_length)
    rank *= dampen_factor
    return rank, get_time


# 각각 페이지에 대하여 rank를 계산하고 dynamodb에 업데이트 합니다.
def ranking_each_page(page, page_relation, iter, remain_page, reader_arr):
    rank_start = time.time()
    rank, get_time = ranking(page_relation, reader_arr)
    page_rank = rank + remain_page
    rank_time = time.time() - rank_start

    return {'iter': iter,
            'page': page,
            'get_time': get_time,
            'rank_time': rank_time,
            'page_rank': page_rank,
            'relation_length': len(page_relation)}


def lambda_handler(event, context):
    current_iter = event['current_iter']
    end_iter = event['end_iter']
    remain_page = event['remain_page']
    file = event['file']
    page_relations = get_s3_object(bucket, file)

    reader_arr = []
    for idx in range(total_divide_num + 1):
        try:
            read_db = db_path + str(idx) + '.db'
            reader = sqlite3.connect(read_db, timeout=600)
            reader.cursor().execute('''CREATE TABLE if not exists pagerank(
                                                page INTEGER NOT NULL PRIMARY KEY,
                                                iter integer ,
                                                rank real,
                                                relation_length integer
                                             )''')
            reader_arr.append(reader)
        except:
            pass
    db_name = file.split('/')[2]
    db_name = int(db_name.split('.')[0])
    writer = sqlite3.connect(db_path + str(db_name) + '.db')
    while current_iter <= end_iter:
        ret = []
        for page, page_relation in page_relations.items():
            ranking_result = ranking_each_page(page, page_relation, current_iter, remain_page, reader_arr)
            result = (ranking_result['page'], ranking_result['iter'], ranking_result['page_rank'],
                      ranking_result['relation_length'])
            ret.append(result)
            print(ranking_result)
        put_start = time.time()
        put_efs(ret, writer)
        put_time = time.time() - put_start
        print(put_time)
        current_iter += 1

    return True
