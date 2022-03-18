import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


engine = create_engine("mysql+mysqlconnector://tc_dev:passwd@127.0.0.1/tc")
# tables = ["storms", "observations", "forecasts", "steps", "tracks"]

# whether or not to remove existing export files
CLOBBER = False
test_dir = "test3"
export_type = "validation"
output_dir = Path(f"/Users/jmiller/Work/tcdb/tests/{test_dir}/{export_type}")
output_dir.mkdir(parents=True, exist_ok=True)
# tables = ["storms", "observations"]
tables = ["storms", "observations", "forecasts", "steps", "tracks"]
for table in tables:
    df = pd.read_sql_table(table, engine)
    output_file = output_dir.joinpath(f"{table}_{export_type}_{test_dir}.csv")
    if output_file.exists():
        if CLOBBER:
            print(f"{output_file.as_posix()} already exists. Replacing existing file now...")
        else:
            print(f"{output_file.as_posix()} already exists. CLOBBER if False, moving to next table")
    if export_type == "init":  # don't include header and represent NaN with \N
        df.to_csv(output_file, index=False, na_rep="\\N", header=False)
    elif export_type == "validation":
        df.to_csv(output_file, index=False)
