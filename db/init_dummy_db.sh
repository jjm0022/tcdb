#!/bin/bash


project_dir="/Users/jmiller/Work/tcdb"
mysql_cmd=`which mysql`

${mysql_cmd} -u root -h 127.0.0.1 -ppasswd -P 3307 tc < ${project_dir}/db/init_db_dummydata.sql
