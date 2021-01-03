import subprocess
import time
from threading import Thread

efs_path = '/mnt/efs/fs1/ap/'
local_path = '/'

test_path = efs_path


def requester(event, context):
    byte_size = int(float(event['bs']))
    start_byte = int(event['start'])
    end_byte = int(event['end'])

    file_write_path = test_path + 'read_file'
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


test_set2 = [
    {'bs': '1', 'start': '0', 'end': '50', 'case': '1'},
    {'bs': '1', 'start': '50', 'end': '100', 'case': '2'}
]

test_set4 = [
    {'bs': '1', 'start': '0', 'end': '25', 'case': '1'},
    {'bs': '1', 'start': '25', 'end': '50', 'case': '2'},
    {'bs': '1', 'start': '50', 'end': '75', 'case': '3'},
    {'bs': '1', 'start': '75', 'end': '100', 'case': '4'}
]

threads_1 = []
for obj in test_set4:
    t = Thread(target=requester, args=({
                                           'bs': obj['bs'],
                                           'start': obj['start'],
                                           'end': obj['end'],
                                           'case': obj['case'],
                                       }, 0))
    t.start()
    threads_1.append(t)
for t in threads_1:
    t.join()
