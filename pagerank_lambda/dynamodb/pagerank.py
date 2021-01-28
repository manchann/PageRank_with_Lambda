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
bucket = "jg-pagerank-bucket"


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


def get_past_pagerank(t, page):
    past_pagerank = t.get_item(Key={'page': str(page)})
    return past_pagerank['Item']


def put_dynamodb_items(page, iter, rank, relation_length):
    rank_table.put_item(
        Item={
            'iter': iter,
            'page': str(page),
            'rank': decimal.Decimal(str(rank)),
            'relation_length': decimal.Decimal(str(relation_length)),
        }
    )


dampen_factor = 0.8


def ranking(page_relation):
    leave_page = 0
    for page in page_relation:
        past_info = get_past_pagerank(rank_table, page)
        leave_page += float(past_info['rank']) / float(past_info['relation_length'])

    leave_page *= dampen_factor
    return leave_page


def each_page(page, page_relation, iter, remain_page):
    page_rank = ranking(page_relation) + remain_page
    put_dynamodb_items(page, iter, page_rank, len(page_relation))
    return True


def invoke_init(page, page_relation, pagerank_init):
    put_dynamodb_items(page, 0, pagerank_init, len(page_relation))
    return True


def lambda_handler(event, context):
    current_iter = event['current_iter']
    end_iter = event['end_iter']
    remain_page = event['remain_page']
    file = event['file']
    page_relations = get_s3_object(bucket, file)
    try:
        pagerank_init = event['pagerank_init']
        t_return = []
        for page, page_relation in page_relations.items():
            t = Thread(target=invoke_init, args=(page, page_relation, pagerank_init))
            t.start()
            t_return.append(t)
        for t in t_return:
            t.join()
    except:
        t_return = []
        for page, page_relation in page_relations.items():
            t = Thread(target=each_page, args=(page, page_relation, current_iter, remain_page))
            t.start()
            t_return.append(t)
        for t in t_return:
            t.join()
    if current_iter < end_iter:
        invoke_lambda(current_iter + 1, end_iter, remain_page, file)
    return True
