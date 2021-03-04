import json
import sqlite3
import os

db_name = 'pagerank.db'
db_path = '/mnt/efs/'


def lambda_handler(event, context):
    conn = sqlite3.connect(db_path + db_name)

    cur = conn.cursor()
    cur.executemany('INSERT INTO pagerank VALUES (?, ?, ?)',
                    [(0, 0.1, 1),
                     (1, 0.01, 2)])
    cur.execute('SELECT * FROM pagerank')
    print(cur.fetchall())
    conn.commit()
    conn.close()


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
