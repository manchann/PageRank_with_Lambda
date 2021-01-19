import boto3
import time
import decimal

dynamodb = boto3.resource('dynamodb')
db_name = 'jg-file-write-lock'
table = dynamodb.Table(db_name)
result = 'jg-timecheck'
result_table = dynamodb.Table(result)


def get_dynamodb_items(t):
    response = t.scan()
    return response['Items']


def put_dynamodb_items(data, isLock):
    result_table.put_item(
        Item={
            'test': str(time.time()),
            'isLock': isLock,
            'total_time': decimal.Decimal(str(data))
        }
    )


def total_time(data):
    start = time.time()
    end = 0
    for d in data:
        if (start > d['start']):
            start = d['start']
        if (end < d['end']):
            end = d['end']
    return end - start


get_data = get_dynamodb_items(table)

F_arr = []
T_arr = []
for data in get_data:
    if (data['isLock'] == 'F'):
        F_arr.append(data)
    elif (data['isLock'] == 'T'):
        T_arr.append(data)

F_total_time = total_time(F_arr)
T_total_time = total_time(T_arr)
put_dynamodb_items(F_total_time, 'F')
put_dynamodb_items(T_total_time, 'T')
