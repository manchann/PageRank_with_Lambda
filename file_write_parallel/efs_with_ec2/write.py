import subprocess
import time
from threading import Thread

efs_path = '/mnt/efs/fs1/'
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

test_set10 = [
    {'bs': '1', 'start': '0', 'end': '10', 'case': '0'},
    {'bs': '1', 'start': '10', 'end': '20', 'case': '1'},
    {'bs': '1', 'start': '20', 'end': '30', 'case': '2'},
    {'bs': '1', 'start': '30', 'end': '40', 'case': '3'},
    {'bs': '1', 'start': '40', 'end': '50', 'case': '4'},
    {'bs': '1', 'start': '50', 'end': '60', 'case': '5'},
    {'bs': '1', 'start': '60', 'end': '70', 'case': '6'},
    {'bs': '1', 'start': '70', 'end': '80', 'case': '7'},
    {'bs': '1', 'start': '80', 'end': '90', 'case': '8'},
    {'bs': '1', 'start': '90', 'end': '100', 'case': '9'},
]

threads_1 = []
for obj in test_set10:
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
