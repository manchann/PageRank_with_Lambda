#!/bin/bash

function_name="jg-file-write-efs"
efs_path="/mnt/efs/ap/read_file"
rm -rf $efs_path
aws lambda update-function-configuration --function-name $function_name --memory-size 256
sleep 5
aws lambda update-function-configuration --function-name $function_name --memory-size 512
sleep 5
python3 ./init.py
sleep 5
python3 ./request.py
sleep 5
python3 ./byte_read.py
