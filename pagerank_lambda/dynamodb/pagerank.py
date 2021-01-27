import boto3
import json
import resource
import time
import decimal
from botocore.client import Config
from threading import Thread

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


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key):
    s3.Bucket(bucket).put_object(Key=key)


def invoke_lambda(page, iter, remain_page):
    '''
    Lambda 함수를 호출(invoke) 합니다.
    '''

    resp = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "page": page,
            "iter": iter,
            "remain_page": remain_page
        })
    )


def get_page_relation(t, page):
    page_relation = t.get_item(Key={'iter': 0, 'page': page})
    return page_relation['Item']


def get_past_pagerank(t, iter, page_relation):
    past_pagerank = []
    for page in page_relation['relation']:
        past_page_info = t.get_item(Key={'iter': iter - 1, 'page': str(page)})
        print('page', str(page))
        print('past_page_info', past_page_info['Item'])
        past_pagerank.append(past_page_info['Item'])
    return past_pagerank


def put_dynamodb_items(page, iter, rank, relation):
    rank_table.put_item(
        Item={
            'iter': iter,
            'page': str(page),
            'rank': decimal.Decimal(str(rank)),
        }
    )


dampen_factor = 0.8


def ranking(page_relation, past_pageranks):
    leave_page = 0
    for p in page_relation['relation']:
        for past in past_pageranks:
            if past['page'] == p:
                past_rank = float(past['rank'])
                leave_page += (past_rank / float(len(past['relation'])))
    leave_page *= dampen_factor

    return leave_page


def each_page(page, iter, remain_page):
    page_relation = get_page_relation(rank_table, page)
    past_pagerank = get_past_pagerank(rank_table, iter, page_relation)
    page_rank = ranking(page_relation, past_pagerank) + remain_page
    put_dynamodb_items(page, iter, page_rank, page_relation['relation'])


def lambda_handler(event, context):
    pages_range = event['pages_range']
    iter = event['iter']
    remain_page = event['remain_page']
    divided_page_num = event['divided_page_num']
    t_return = []
    for page in range(pages_range, pages_range + divided_page_num):
        t = Thread(target=each_page, args=(str(page), iter, remain_page))
        t.start()
        t_return.append(t)
    for t in t_return:
        t.join()
    return str(iter) + '번째 ' + str(pages_range) + '범위 완료'
