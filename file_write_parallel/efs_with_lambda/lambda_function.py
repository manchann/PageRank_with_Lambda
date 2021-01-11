import time

mnt_test = '/mnt/efs/'


def lambda_handler(event, context):
    start = time.time()
    byte_size = int(float(event['bs']))
    start_byte = int(event['start'])
    end_byte = int(event['end'])

    file_write_path = mnt_test + 'read_file'
    case = str(event['case']).encode()
    with open(file_write_path, 'r+b', 0) as f:
        for idx in range(start_byte, end_byte):
            f.seek(byte_size * idx)
            f.write(case)
        f.close()
    end = time.time()
    return {
        'start_byte': start_byte,
        'end_byte': end_byte,
        'start_time': start,
        'end_time': end
    }
