#!/bin/bash

. ../.env # export environment variables

project_dir=${TCDB_HOME}
mysql_cmd=`which mysql`

#${mysql_cmd} -u root -h 127.0.0.1 -ppasswd -P 3307 tc < ${project_dir}/db/init_db_dummydata.sql
#${mysql_cmd} -u ${TCDB_USER} -h ${TCDB_HOST} -p${TCDB_PW} ${TCDB_DB} < ${project_dir}/db/init_db_dummydata.sql  # mysql on plexserver
