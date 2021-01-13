import subprocess
import time
import boto3
from threading import Thread

dynamodb = boto3.resource('dynamodb')
db_name = 'jg-file-write'
table = dynamodb.Table(db_name)


def dynamodb_remove_all_items():
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={
                'case': each['case'],
            })


def requester(bs, start, end, case):
    subprocess.check_call(
        "\curl 'https://a6o7mwk0ob.execute-api.us-west-2.amazonaws.com/first/?bs=%22'{}'%22&start=%22'{}'%22&end=%22'{}'%22&case=%22'{}'%22#'".format(
            bs, start, end, case),
        shell=True)


test_set2 = [
    {'bs': '1', 'start': '0', 'end': '50', 'case': '1'},
    {'bs': '1', 'start': '50', 'end': '100', 'case': '2'}
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
#
# for obj in test_set:
#     requester(obj['bs'], obj['start'], obj['end'], obj['case'])
dynamodb_remove_all_items()
for obj in test_set10:
    t = Thread(target=requester, args=(obj['bs'], obj['start'], obj['end'], obj['case']))
    t.start()
    threads_1.append(t)
for t in threads_1:
    t.join()
