#!/bin/bash

function_name="jg-file-write-efs"
handler="lambda_function lambda_function_lock"
efs_path="/mnt/efs/ap/read_file"

iter=20
iteration=$(seq 0 $iter)

for i in $iteration; do
  for hd in $handler; do
    sudo rm -rf $efs_path
    aws lambda update-function-configuration --function-name $function_name --handler $hd.lambda_handler --memory-size 256
    sleep 5
    aws lambda update-function-configuration --function-name $function_name --memory-size 512
    sleep 2
    sudo python3 ./init.py
    sleep 2
    sudo python3 ./request.py
    sleep 2
    sudo python3 ./byte_read.py
    sleep 10
  done
  sudo python3 ./get_dynamodb.py
  sleep 5
done
