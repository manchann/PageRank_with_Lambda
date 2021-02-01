import json
import boto3
import glob
import subprocess
import lambdautils
import decimal
from threading import Thread
import time
from botocore.client import Config
from boto3.dynamodb.types import DYNAMODB_CONTEXT

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

config = json.loads(open('driverconfig.json', 'r').read())

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

bucket = config["bucket"]
relation_prefix = config["relationPrefix"]
region = config["region"]

db_name = 'jg-page-relation' + '-' + config['pages']
table = dynamodb.Table(db_name)

pages_list = []


def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


# case: heavy data
divided_page_num = config["divided_page_num"]
page_file = s3_client.get_object(Bucket=bucket, Key=config["pages"])
page_file = page_file['Body'].read().decode()


# case: light data
# p = s3_client.get_object(Bucket=bucket, Key=config['pages'])
# pages_list.append(p)


# page들의 관계 데이터셋을 만들어 반환하는 함수 입니다.
def get_page_relation(file, pages):
    page_relations = {}
    lines = pages.split("\n")
    page = divided_page_num * file
    is_start = False
    for line in lines:
        try:
            key = line.split("\t")[0]
            value = line.split("\t")[1]
            value = value.replace("\r", "")
            if key == value:
                continue
            key_compared = int(key)
            if key_compared == page:
                is_start = True
                if key not in page_relations:
                    page_relations[key] = []
                if value not in page_relations[key]:
                    page_relations[key].append(value)
                    print(file + '번째 ' + key + ' ' + value + '완료')
            elif key_compared > page:
                page += 1
            if is_start is True and page >= divided_page_num * (idx + 1):
                break
        except:
            pass
    if len(page_relations) > 0:
        write_to_s3(bucket, config['relationPrefix'] + str(file) + '.txt',
                    json.dumps(page_relations).encode(), {})

    return True


# page의 관계들이 담겨있는 파일을 가지고 dictionary 관계 데이터셋을 만듭니다.
thread_list = []
for d in range(20):
    for idx in range(10 * d, 10 * (d + 1)):
        t = Thread(target=get_page_relation, args=(idx, page_file,))
        t.start()
        thread_list.append(t)
    for thr in thread_list:
        thr.join()

    print('----------------- ' + str(d) + '번째 분할 끝 -----------------')
