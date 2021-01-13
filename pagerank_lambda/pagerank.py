import boto3
import json
import resource
import time
import decimal

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
db_name = 'jg-pagerank'
table = dynamodb.Table(db_name)


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key):
    s3.Bucket(bucket).put_object(Key=key)


def get_dynamodb_items(iter):
    response = table.scan()
    past_pageranks = []
    for res in response['Items']:
        if res['iter'] == iter - 1:
            past_pageranks.append(res)
    return past_pageranks


def put_dynamodb_items(page, iter, rank):
    table.put_item(
        Item={
            'iter': iter,
            'page': str(page),
            'rank': decimal.Decimal(str(rank))
        }
    )


dampen_factor = 0.8


def ranking(page, page_relations, past_pageranks):
    unconnect_page = (1 - dampen_factor) / len(page_relations)
    connect_page = 0
    for p in page_relations[page]:
        for past in past_pageranks:
            if past['page'] == p:
                past_rank = float(past['rank'])
                connect_page += (past_rank / len(page_relations[p]))
    connect_page *= dampen_factor

    page_rank = unconnect_page + connect_page
    return page_rank


def lambda_handler(event, context):
    bucket = event['bucket']
    page = event['page']
    page_relation = event['page_relation']
    iter = event['iter']
    past_pageranks = get_dynamodb_items(iter)

    page_rank = ranking(page, page_relation, past_pageranks)

    put_dynamodb_items(page, iter, page_rank)
    return page_rank

    # case: S3
    # file_read_path = str(event['iter']) + '.txt'
    # with open(file_read_path, 'r+b', 0) as f:
    #     f.seek(int(page))
    #     page_rank = ranking(page)
    #     f.write(page_rank)
