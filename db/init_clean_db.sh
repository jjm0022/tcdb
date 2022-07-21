#!/bin/bash


project_dir=$(dirname $0)
echo $project_dir
mysql_cmd=`which mysql`

#${mysql_cmd} -u root -h 127.0.0.1 -ppasswd -P 3307 tc < ${project_dir}/db/init_db_base.sql
${mysql_cmd} -u root -h 192.168.1.144 -pb14z3r5 tc < ${project_dir}/init_db_base.sql  # mysql on plexserver
${mysql_cmd} -u root -h 192.168.1.144 -pb14z3r5 tc < ${project_dir}/insert_full_models.sql  # mysql on plexserver
