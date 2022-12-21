#!/bin/bash

cd /home/jmiller/tcdb/tcdb/pipeline

/usr/bin/env /home/jmiller/anaconda3/envs/tcdb/bin/python -c 'from routines import updateActiveSystems; updateActiveSystems()'