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

total_pages = []


def sort_by_destination(line):
    try:
        line = line.split('\t')
        destination = int(line[1].replace("\r", ""))
        return destination
    except:
        return 0


# page들의 관계 데이터셋을 만들어 반환하는 함수 입니다.
def get_page_relation(file, pages):
    page_relations = {}
    page = divided_page_num * file
    is_start = False
    for line in pages:
        try:
            source = line.split("\t")[0]
            destination = line.split("\t")[1].replace("\r", "")
            if source == destination:
                continue
            key_compared = int(destination)
            while key_compared > page:
                page += 1
            if is_start is True and page >= divided_page_num * (file + 1):
                break
            if key_compared == page:
                is_start = True
                if destination not in page_relations:
                    page_relations[destination] = []
                    total_pages.append(destination)
                if source not in page_relations[destination]:
                    page_relations[destination].append(source)
                    total_pages.append(source)
        except:
            pass
    if file == 490:
        print(page_relations)
    if len(page_relations) > 0:
        write_to_s3(bucket, config['relationPrefix'] + str(file) + '.txt',
                    json.dumps(page_relations).encode(), {})

    return True


page_file = page_file.split("\n")
# for idx in range(len(page_file)):
#     if len(page_file[idx]) == 0:
#         del page_file[idx]
page_file.sort(key=sort_by_destination)
# page의 관계들이 담겨있는 파일을 가지고 dictionary 관계 데이터셋을 만듭니다.
thread_list = []
last_destination = page_file[-1].split("\t")[1].replace("\r", "")
print('last_destination: ', last_destination)
loop = int(last_destination) / (divided_page_num * 10)
loop = int(loop)
print('총 반복 횟수: ', loop)
start = time.time()

for d in range(loop):
    for idx in range(10 * d, 10 * (d + 1)):
        t = Thread(target=get_page_relation, args=(idx, page_file,))
        t.start()
        thread_list.append(t)
    for thr in thread_list:
        thr.join()

    print('----------------- ' + str(d) + '번째 분할 끝 -----------------')
    print('현재까지 총 걸린 시간: ', time.time() - start)

total_pages = set(total_pages)
total_pages = list(total_pages)

write_to_s3(bucket, config['relationPrefix'] + 'total_page.txt', json.dumps(total_pages), {})
