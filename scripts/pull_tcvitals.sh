#!/bin/bash

output_dir='/Work_Data/tcdb/data/lake/atcf/tcvitals'
date_time=`date +%Y%m%dT%H%M`
year=`date +%Y`
output_file="${output_dir}/tcvitals_${date_time}"

echo "Downloading tcvitals file to check for updates"

wget https://ftp.nhc.noaa.gov/atcf/com/tcvitals -q -O ${output_file} 

for file in ${output_dir}/tcvitals_${year}*; do
    if [[ $file -ef  $output_file ]]; then
        continue
    else
        # compare file contents
        /usr/bin/cmp $file $output_file
        if [[ $? -eq 0 ]]; then
            echo "${output_file##*/} is the same as ${file##*/}"
            rm -v $output_file
            break
        fi
    fi
done

if [[ -e $output_file ]]; then
    echo "Saving updated tcvitals file to ${output_file}"
else
    echo "No updates for tcvitals file"
fi

echo "========================================================================"