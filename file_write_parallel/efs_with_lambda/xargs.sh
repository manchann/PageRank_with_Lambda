#!/bin/bash
HOST="https://w8ib1d0lv2.execute-api.us-east-1.amazonaws.com/lambda-test/lambda-test"
rm -rf /mnt/data/out.text
sudo dd if=/dev/zero of=/mnt/data/out.text bs=1 count=1024
sudo chown 1001:1001 /mnt/data/out.text
sudo chmod 644 /mnt/data/out.text
begin=$(date +%s%3N) seq 1 10 | xargs -n1 -P10 -I % curl -X POST -H "Content-Type: application/json" --data '{"sequence_id":'%'}' $HOST -s end=$(date +%s%3N) latency=$((end - begin)) echo 'total time: '$latency >/dev/null >>/mnt/data/out.text
