import time
import boto3
import decimal

mnt_test = '/mnt/efs/'
dynamodb = boto3.resource('dynamodb')
db_name = 'jg-file-write-lock'
table = dynamodb.Table(db_name)


def put_dynamodb_items(case, data):
    table.put_item(
        Item={
            'start': decimal.Decimal(data[0]),
            'end': decimal.Decimal(data[1]),
            'case': case,
            'isLock': 'F'
        }
    )


def lambda_handler(event, context):
    time_datas = []
    byte_size = int(float(event['bs']))
    start_byte = int(event['start'])
    end_byte = int(event['end'])

    file_write_path = mnt_test + 'read_file'
    case = str(event['case']).encode()
    current_time = time.time()
    time_datas.append(current_time)
    with open(file_write_path, 'r+b', 0) as f:
        for idx in range(start_byte, end_byte):
            f.seek(byte_size * idx)
            f.write(case)
        f.close()
    current_time = time.time()
    time_datas.append(current_time)
    put_dynamodb_items(str(event['case']), time_datas)
    return {
        'case': str(event['case'])
    }
