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

db_name = 'test.db'
db_path = '/mnt/efs/ap/'


def put(db):
    db = db_path + db
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    cur.execute('''CREATE TABLE if not exists test(
                             name TEXT NOT NULL PRIMARY KEY
                          )''')

    name = str(time.time())
    print(name)
    cur.execute('INSERT OR REPLACE INTO test VALUES (?)', (name,))
    conn.commit()


t_return = []
for idx in range(2):
    t = Thread(target=put, args=(str(idx),))
    t.start()
    t_return.append(t)
for t in t_return:
    t.join()
