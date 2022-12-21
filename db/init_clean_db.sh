#!/bin/bash

. ../.env # export environment variables

project_dir=${TCDB_HOME}
echo $project_dir
mysql_cmd=`which mysql`

#${mysql_cmd} -u root -h 127.0.0.1 -ppasswd -P 3307 tc < ${project_dir}/db/init_db_base.sql
#${mysql_cmd} -u ${TCDB_USER} -h ${TCDB_HOST} -p${TCDB_PW} ${TCDB_DB} < ${project_dir}/init_db_base.sql  # mysql on plexserver
#${mysql_cmd} -u ${TCDB_USER} -h ${TCDB_HOST} -p${TCDB_PW} ${TCDB_DB} < ${project_dir}/insert_full_models.sql  # mysql on plexserver
