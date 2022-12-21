#!/bin/bash

. ../.env # export environment variables

project_dir=${TCDB_HOME}
mysql_cmd=`which mysql`
tmp_dir='/Work_Data/tcdb/data/tmp'

#${mysql_cmd} -u ${TCDB_USER} -p${TCDB_PW} -h ${TCDB_HOST} tc --batch --raw < ${project_dir}/db/scripts/getSeasonStorms.sql > ${tmp_dir}/storms.tsv

#cd /home/jmiller/tcdb/tcdb/pipeline

#/usr/bin/env /home/jmiller/anaconda3/envs/tcdb/bin/python bdeck.py
