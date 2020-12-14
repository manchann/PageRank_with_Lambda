import subprocess
import time
from threading import Thread

efs_path = '/mnt/efs/'
local_path = './'

test_path = local_path


def requester(event, context):
    byte_size = int(float(event['bs']))
    start_byte = int(event['start'])
    end_byte = int(event['end'])

    file_write_path = test_path + 'read_file'
    arr = []
    with open(file_write_path, 'wb', 0) as f:
        for idx in range(start_byte, end_byte):
            arr.append(idx)
            f.seek(byte_size * idx)
            f.write(str(event['case']).encode())
        f.close()
    return {
        'start_byte': start_byte,
        'end_byte': end_byte,
    }


test_set = [
    {'bs': '1024', 'start': '0', 'end': '256', 'case': '1'},
    {'bs': '1024', 'start': '256', 'end': '512', 'case': '2'},
    {'bs': '1024', 'start': '512', 'end': '768', 'case': '3'},
    {'bs': '1024', 'start': '768', 'end': '1024', 'case': '4'},
]

test_set2 = [
    {'bs': '1', 'start': '0', 'end': '50', 'case': '1'},
    {'bs': '1', 'start': '50', 'end': '100', 'case': '2'}
]

test_init = [
    {'bs': '1', 'start': '0', 'end': '100', 'case': '0'}
]

test_set3 = [
    {'bs': '1024', 'start': '0', 'end': '128', 'case': '1'},
    {'bs': '1024', 'start': '128', 'end': '256', 'case': '2'},
    {'bs': '1024', 'start': '256', 'end': '384', 'case': '3'},
    {'bs': '1024', 'start': '384', 'end': '512', 'case': '4'},
    {'bs': '1024', 'start': '512', 'end': '640', 'case': '5'},
    {'bs': '1024', 'start': '640', 'end': '768', 'case': '6'},
    {'bs': '1024', 'start': '768', 'end': '896', 'case': '7'},
    {'bs': '1024', 'start': '896', 'end': '1024', 'case': '8'},
]

threads_1 = []
for obj in test_set2:
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
