import subprocess
import time
from threading import Thread


def requester(bs, start, end, case):
    subprocess.check_call(
        "\curl 'https://yq7pnajel1.execute-api.us-east-1.amazonaws.com/first/?bs=%22'{}'%22&start=%22'{}'%22&end=%22'{}'%22&case=%22'{}'%22#'".format(
            bs, start, end, case),
        shell=True)


test_set = [
    {'bs': '1024', 'start': '0', 'end': '256', 'case': '1'},
    {'bs': '1024', 'start': '256', 'end': '512', 'case': '2'},
    {'bs': '1024', 'start': '512', 'end': '768', 'case': '3'},
    {'bs': '1024', 'start': '768', 'end': '1024', 'case': '4'},
]

threads_1 = []
#
# for obj in test_set:
#     requester(obj['bs'], obj['start'], obj['end'], obj['case'])

for obj in test_set:
    t = Thread(target=requester, args=(obj['bs'], obj['start'], obj['end'], obj['case']))
    t.start()
    threads_1.append(t)
for t in threads_1:
    t.join()
