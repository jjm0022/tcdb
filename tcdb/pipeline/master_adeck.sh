#!/bin/bash

cd /home/jmiller/tcdb/tcdb/pipeline
date_time=$(date +%Y%m%d%H)
echo "Using current datetime: ${date_time}"
sleep 5
/usr/bin/env /home/jmiller/anaconda3/envs/tcdb/bin/python adeck.py -d ${date_time}
