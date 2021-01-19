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
relation_name = 'jg-page-relation'
table = dynamodb.Table(db_name)
relation_table = dynamodb.Table(relation_name)


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key):
    s3.Bucket(bucket).put_object(Key=key)


def get_dynamodb_items(t, iter):
    response = t.scan()
    past_pageranks = []
    if (iter > 0):
        for res in response['Items']:
            if res['iter'] == iter - 1:
                past_pageranks.append(res)
        return past_pageranks
    else:
        return response['Items']


def put_dynamodb_items(page, iter, rank, relation):
    table.put_item(
        Item={
            'iter': iter,
            'page': str(page),
            'rank': decimal.Decimal(str(rank)),
        }
    )


def get_page_relation(page, past_pageranks):
    page_relation = get_dynamodb_items(relation_table, 0)
    for past in page_relation:
        if past['page'] == page:
            return past['relation']


dampen_factor = 0.8


def ranking(page_relation, past_pageranks):
    leave_page = 0
    for p in page_relation:
        for past in past_pageranks:
            if past['page'] == p:
                past_rank = float(past['rank'])
                leave_page += (past_rank / float(len(past['relation'])))
    leave_page *= dampen_factor

    return leave_page


def lambda_handler(event, context):
    page = event['page']
    iter = event['iter']
    remain_page = event['remain_page']
    past_pageranks = get_dynamodb_items(table, iter)

    page_relation = get_page_relation(page, past_pageranks)
    page_rank = ranking(page_relation, past_pageranks) + remain_page

    put_dynamodb_items(page, iter, page_rank, page_relation)
    return page_rank

    # case: S3
    # file_read_path = str(event['iter']) + '.txt'
    # with open(file_read_path, 'r+b', 0) as f:
    #     f.seek(int(page))
    #     page_rank = ranking(page)
    #     f.write(page_rank)
