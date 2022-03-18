#!/bin/bash

year=${1}
url='https://ftp.nhc.noaa.gov/atcf/archive/'

cd /tmp

for year in {2000..2019}; do
    wget -nv -r --no-parent -l1 ${url}/${year}/
done
