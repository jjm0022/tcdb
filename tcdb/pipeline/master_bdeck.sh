#!/bin/bash

cd /home/jmiller/tcdb/tcdb/pipeline

hour=$(date +%H)
cycles=('00' '06' '12' '18') 
if [[ " ${cycles[*]} " =~ " ${hour} " ]]; then # force update the db using the most recent bdeck
   /usr/bin/env /home/jmiller/anaconda3/envs/tcdb/bin/python bdeck.py -f
else
   /usr/bin/env /home/jmiller/anaconda3/envs/tcdb/bin/python bdeck.py
fi

