import requests
import re
from pathlib import Path
from datetime import datetime
import logging

from tempfile import TemporaryDirectory

now = datetime.now()

BDECK_PATTERN = search_pattern = f'b[aec][lp][012349][0123456789]{now.year}.dat'
BDECK_URL = 'https://ftp.nhc.noaa.gov/atcf/btk/'

def getFileNames(response):
    pattern = re.compile(BDECK_PATTERN)
    file_list = pattern.findall(response.text)
    response.close()
    return set(file_list) 

def downloadLocally(url, local_path):
    try:
        response = requests.get(url)
        with open(local_path, 'w') as f:
            f.write(response.text)
    except Exception as e:
        logging.error(e)
        return False
    return True

if __name__ == "__main__":
    data_lake = Path(f"/Users/jmiller/lake/bdeck/{now.year}")
    data_lake.mkdir(parents=True, exist_ok=True)

    response = requests.get(BDECK_URL)
    file_names = getFileNames(response)
    downloaded_files = list()
    with TemporaryDirectory() as tmp_dir:
        for file_name in file_names:
            most_recent_file_date = datetime(2000, 1, 1)
            file_url = BDECK_URL + file_name
            tmp_path = tmp_dir.joinpath(file_name)
            if downloadLocally(file_url, tmp_path):
                hist_files = list(data_lake.glob(f"{file_name.split('.')[0]}_*"))
                if len(hist_files) > 0:
                    file_times = {now.timestamp() - hf.lstat().st_mtime: hf for hf in hist_files}
                else:
                    
                
                for hf in hist_files:
                    file_times


            
    

                


