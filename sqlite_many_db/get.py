import json
import boto3
import glob
import os
import subprocess
import decimal
import time
from botocore.client import Config
from boto3.dynamodb.types import DYNAMODB_CONTEXT
from threading import Thread
import fcntl
import sqlite3
import sys

# thread_count = int(sys.argv[1])

db_name = 'test.db'
db_path = '/mnt/efs/ap/'


def get(write):
    for db in range(20):
        db = db_path + str(db) + '.db'
        conn = sqlite3.connect(db, timeout=900, check_same_thread=False)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE if not exists test(
                                 name TEXT NOT NULL PRIMARY KEY
                              )''')

        cur.execute('SELECT * FROM test')
        conn.close()
    writer = db_path + str(write) + '.db'
    conn = sqlite3.connect(writer, timeout=900, check_same_thread=False)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE if not exists test(
                                     name TEXT NOT NULL PRIMARY KEY
                                  )''')

    name = str(time.time())
    cur.execute('INSERT OR REPLACE INTO test VALUES (?)', (name,))
    conn.commit()


for thread_count in range(1,21):
    t_return = []
    start = time.time()
    for idx in range(thread_count):
        t = Thread(target=get, args=(str(idx),))
        t.start()
        t_return.append(t)
    for t in t_return:
        t.join()
    print(time.time() - start)
    # print('-------------------------- ' + str(thread_count) + ' ----------------------------')
    time.sleep(1)
