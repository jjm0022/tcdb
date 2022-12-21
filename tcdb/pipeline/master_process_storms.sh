#!/bin/bash

project_dir='/home/jmiller/tcdb'
mysql_cmd=`which mysql`
tmp_dir='/Work_Data/tcdb/data/tmp'

source "${project_dir}/db/scripts/.env_vars"
${mysql_cmd} -u ${MYSQL_USER} -pb14z3r5 -h ${MYSQL_HOST} tc --batch --raw < ${project_dir}/db/scripts/getSeasonStorms.sql > ${tmp_dir}/storms.tsv

#cd /home/jmiller/tcdb/tcdb/pipeline

#/usr/bin/env /home/jmiller/anaconda3/envs/tcdb/bin/python bdeck.py
