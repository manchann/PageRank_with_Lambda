#!/bin/bash

efs_path="/mnt/efs/ap/read_file"
sudo rm -rf $efs_path
sleep 2
sudo python3 ./init.py
sleep 2
sudo python3 ./write.py
sleep 2
sudo python3 ./byte_read.py
