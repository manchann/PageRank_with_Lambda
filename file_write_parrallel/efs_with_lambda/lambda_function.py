import time

mnt_test = '/mnt/efs/'


def lambda_handler(event, context):
    byte_size = int(float(event['bs']))
    start_byte = int(event['start'])
    end_byte = int(event['end'])

    file_write_path =mnt_test + 'read_file'
    arr = []
    with open(file_write_path, 'r+b', 0) as f:
        for idx in range(start_byte, end_byte):
            arr.append(idx)
            f.seek(byte_size * idx)
            f.write(str(event['case']).encode())
        f.close()
    return {
        'start_byte': start_byte,
        'end_byte': end_byte,
    }
