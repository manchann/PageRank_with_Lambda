import time
import boto3
import decimal
import fcntl

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
            'isLock': 'T'
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
        # file lock : start_byte 부터 10개의 byte 범위를 lock
        fcntl.lockf(f, fcntl.LOCK_EX, 10, start_byte, 1)
        for idx in range(start_byte, end_byte):
            f.seek(byte_size * idx)
            f.write(case)
        # file lock : start_byte 부터 10개의 byte 범위를 unlock
        fcntl.lockf(f, fcntl.LOCK_UN, start_byte, 1)
        f.close()
    current_time = time.time()
    time_datas.append(current_time)
    put_dynamodb_items(str(event['case']), time_datas)

    return {
        'case': str(event['case'])
    }
