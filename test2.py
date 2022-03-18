# coding: utf-8
from tcdb.etl.backfill_storms import process_archive_data
from datetime import datetime
from pathlib import Path

#process_adecks('AL', input_dir=Path('/Users/jmiller/Work/tcdb_pipeline/tests/test1/'))
#process_syntracks('AL', "ECMWF", datetime(2021, 9, 24, 6))
for year in range(2000, 2020):
    process_archive_data(Path(f"/tmp/ftp.nhc.noaa.gov/atcf/archive/{year}/"))
