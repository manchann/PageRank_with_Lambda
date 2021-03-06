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
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
db_name = 'jg-sqlite-test'
table = dynamodb.Table(db_name)

lambda_read_timeout = 300
boto_max_connections = 1000
lambda_config = Config(read_timeout=lambda_read_timeout, max_pool_connections=boto_max_connections,
                       retries={'max_attempts': 0})
lambda_client = boto3.client('lambda', region_name='us-west-2', config=lambda_config)
lambda_name = 'jg-sqlite-pagerank'
bucket = "jg-pagerank-bucket2"

db_path = '/mnt/efs/ap/'


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


def get_past_pagerank(get_query_arr, reader_arr, idx):
    ret = []
    for idx in range(len(get_query_arr)):
        if get_query_arr[idx] == '0':
            continue
        get_query_arr[idx] = get_query_arr[idx][:len(get_query_arr[idx]) - 4] + ';'
        reader = reader_arr[idx]
        cur = reader.cursor()
        print(cur)
        cur.execute(get_query_arr[idx])
        res = cur.fetchall()
        ret += res
        print(ret)
    return ret


def put_efs(data, conn, idx):
    start = time.time()
    print(idx + ' put try ', start)
    cur = conn.cursor()
    cur.executemany("REPLACE INTO 'pagerank' VALUES (?, ?, ?, ?)",
                    data)
    print(idx + ' put execute fin ', time.time(), time.time() - start)
    conn.commit()
    print(idx + ' put commit fin ', time.time(), time.time() - start)
    return True


dampen_factor = 0.8


# 랭크를 계산합니다.
def ranking(page_relation, conn, idx):
    rank = 0

    get_query_arr = ['0' for i in range(total_divide_num + 1)]
    page_query = "SELECT * FROM 'pagerank' Where "
    for page in page_relation:
        # dynamodb에 올려져 있는 해당 페이지의 rank를 가져옵니다.
        db_num = int(page) // 1000
        if get_query_arr[db_num] == '0':
            get_query_arr[db_num] = page_query
        get_query_arr[db_num] += 'page=' + page + ' OR '
    get_start = time.time()
    past_pagerank = get_past_pagerank(get_query_arr, conn, idx)
    get_time = time.time() - get_start

    for page_data in past_pagerank:
        past_rank = page_data[2]
        relation_length = page_data[3]
        rank += (past_rank / relation_length)
    rank *= dampen_factor
    return rank, get_time


# 각각 페이지에 대하여 rank를 계산하고 dynamodb에 업데이트 합니다.
def ranking_each_page(page, page_relation, iter, remain_page, conn, idx):
    rank_start = time.time()
    rank, get_time = ranking(page_relation, conn, idx)
    page_rank = rank + remain_page
    rank_time = time.time() - rank_start
    # put_start = time.time()
    # put_efs(page, page_rank, iter, len(page_relation), conn)
    # put_time = time.time() - put_start
    return (page,
            iter,
            page_rank,
            len(page_relation))


def put_dynamodb(data):
    for d in data:
        table.put_item(
            Item={
                'page': str(d[0]),
                'iter': d[2]
            }
        )


total_divide_num = 4840


def lambda_handler(current_iter, end_iter, remain_page, file, idx):
    start = time.time()
    page_relations = get_s3_object(bucket, file)
    print('s3 get 걸린 시간', time.time() - start)
    print(idx, ' connect try')
    # conn = sqlite3.connect(db_path, timeout=600, check_same_thread=False)
    print(idx, 'connect success')
    reader_arr = []
    for i in range(total_divide_num + 1):
        try:
            read_db = db_path + str(i) + '.db'
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
    try:
        db_name = file.split('/')[2]
        db_name = int(db_name.split('.')[0])
        writer = reader_arr[db_name]
        while current_iter <= end_iter:
            print(str(idx) + ' ' + str(current_iter) + '번째 iteration')
            ret = []
            for page, page_relation in page_relations.items():
                ranking_result = ranking_each_page(page, page_relation, current_iter, remain_page, reader_arr, idx)
                ret.append(ranking_result)
            put_efs(ret, writer, idx)
            current_iter += 1
    except Exception as e:
        print(str(idx) + ' error: ', e)
        print(file)


config = json.loads(open('driverconfig.json', 'r').read())

start = time.time()
t_return = []
for idx in range(3):
    start_th = time.time()
    s3_file_path = config['relationPrefix'] + str(idx) + '.txt'
    print(idx, '번째 invoking', time.time() - start_th)
    t = Thread(target=lambda_handler,
               args=(1, 3, 1, s3_file_path, str(idx)))
    st = time.time()
    t.start()
    t_return.append(t)
    print('test: ', time.time() - st)
for t in t_return:
    t.join()
print('걸린 시간: ', time.time() - start)
