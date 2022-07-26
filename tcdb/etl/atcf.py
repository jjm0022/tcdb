import gzip
import pathlib
import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path
from io import BytesIO, StringIO
from datetime import timezone, datetime, timedelta

from tcdb.utils import get_storm_type 
import tcdb.validation as val
import warnings
warnings.filterwarnings("ignore")


UTC = timezone.utc


def parse_aDeck(path):
    """Parse NHC ADeck file

    Args:
        path (pathlib.Path): Path to the ADeck file


    Returns:
        pandas.DataFrame
    """

    # https://www.nrlmry.navy.mil/atcf_web/docs/database/new/abdeck.txt
    header_names = [
        "BASIN",
        "SNUM",
        "DATETIME",
        "TECHNUM/MIN",
        "TECH",
        "TAU",
        "LAT",
        "LON",
        "VMAX",
        "MSLP",
        "TY",
        "RAD",
        "WINDCODE",
        "NE",
        "SE",
        "SW",
        "NW",
    ]

    # with open(file_name, 'rb') as file_handle:
    if path.name.endswith('.gz'):
        string_buffer = compressed_atcf_to_strio(path)
        df = pd.read_csv(
            string_buffer,
            names=header_names,
            index_col=False,
            na_values=["", " ", " " * 2],
            usecols=range(0, len(header_names)),
        )
    else:
        df = pd.read_csv(
            path,
            names=header_names,
            index_col=False,
            na_values=["", " ", " " * 2],
            usecols=range(0, len(header_names)),
        )


    df["DATETIME"] = pd.to_datetime(df.DATETIME, format="%Y%m%d%H")
    # df["DATETIME"] = df["DATETIME"].dt.tz_localize(UTC)
    # We only need current and past dates
    df["LAT"] = df.LAT.apply(lambda x: float(x[:-1]) / 10 if x.endswith("N") else -float(x[:-1]) / 10)
    df["LON"] = df.LON.apply(lambda x: -float(x[:-1]) / 10 if x.endswith("W") else float(x[:-1]) / 10)
    for var in ["VMAX", "MSLP", "NE", "SE", "SW", "NW"]:
        # deal with empty strings
        if df[var].dtype == object:
            # remove spaces -> convert empty string to nan -> convert to float
            df[var] = df[var].str.strip().apply(lambda x: np.nan if x == "" else x).astype(float)
        else:
            df[var] = df[var].astype(float)
    # clean up strings
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.strip()

    return df


def parse_bDeck(path):
    """Parse NHC BDeck file

    Args:
        path (pathlib.Path): Path to the BDeck file

    Returns:
        pandas.DataFrame
    """
    # https://www.nrlmry.navy.mil/atcf_web/docs/database/new/abdeck.txt
    header_names = [
        "BASIN",
        "SNUM",
        "DATETIME",
        "TECHNUM/MIN",
        "TECH",
        "TAU",
        "LAT",
        "LON",
        "VMAX",
        "MSLP",
        "TY",
        "RAD",
        "WINDCODE",
        "NE",
        "SE",
        "SW",
        "NW",
        "POUTER",
        "ROCI",
        "RMW",
        "GUSTS",
        "EYE",
        "SUBREGION",
        "MAXSEAS",
        "INITIALS",
        "DIR",
        "SPEED",
        "STORMNAME",
        "DEPTH",
        "SEAS",
        "SEASCODE",
        "SEAS1",
        "SEAS2",
        "SEAS3",
        "SEAS4",
        "USERDEFINED",
        "userdata",
    ]

    if path.name.endswith(".gz"):
        string_buffer = compressed_atcf_to_strio(path)
    else:
        string_buffer = raw_atcf_to_strio(path)

    df = pd.read_csv(string_buffer, names=header_names, index_col=False, na_values=["", " ", " " * 2])
    if df.empty:
        logger.warning(f"Unable to parse the file: {path.name}")
        return df

    df["DATETIME"] = pd.to_datetime(df.DATETIME, format="%Y%m%d%H")
    # df["DATETIME"] = df["DATETIME"].dt.tz_localize(UTC)
    # We only need current and past dates
    df["LAT"] = df.LAT.apply(lambda x: float(x[:-1]) / 10 if x.endswith("N") else -float(x[:-1]) / 10)
    df["LON"] = df.LON.apply(lambda x: -float(x[:-1]) / 10 if x.endswith("W") else float(x[:-1]) / 10)
    for var in ["VMAX", "MSLP", "NE", "SE", "SW", "NW", "POUTER", "ROCI", "RMW"]:
        # deal with empty strings
        if df[var].dtype == object:
            # remove spaces -> convert empty string to nan -> convert to float
            df[var] = df[var].str.strip().apply(lambda x: np.nan if x == "" else x).astype(float)
        else:
            df[var] = df[var].astype(float)
    # clean up strings
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.strip()

    if not isinstance(df.STORMNAME.values[-1], str):
        df.STORMNAME = df.STORMNAME.mode().values[0]

    return df


def compressed_atcf_to_strio(path):
    """Open and convert a compressed gzip file to text that is then converted to
    StringIO buffer can be processed by pandas

    Args:
        path (pathlib.Path)

    Returns:
        io.StringIO: String buffer that can be read by pandas
    """
    with open(path, "rb") as bi:
        sio_buffer = BytesIO(bi.read())
        gzf = gzip.GzipFile(fileobj=sio_buffer)
        data = gzf.read()
        content = data.splitlines()
        content = [line.decode("utf-8") for line in content]
    return parse_uneven_rows("\n".join(content))


def compressed_atcf_to_text(path):
    """Open and convert a compressed gzip file to text that is then converted to
    StringIO buffer

    Args:
        path (pathlib.Path)

    Returns:
        string
    """
    with open(path, "rb") as bi:
        sio_buffer = BytesIO(bi.read())
        gzf = gzip.GzipFile(fileobj=sio_buffer)
        data = gzf.read()
        content = data.splitlines()
        content = [line.decode("utf-8") for line in content]
    return "\n".join(content)


def raw_atcf_to_strio(path):
    """Simple wrapper for 'parse_uneven_rows' to keep function names consistent.
    Can be processed by pandas

    Args:
        path (pathlib.Path)

    Returns:
        io.StringIO: String buffer that can be read by pandas
    """
    with open(path, "r") as txt:
        text = txt.read()
    return parse_uneven_rows(text)


def parse_uneven_rows(text):
    """Parse NHC ADeck/BDeck text data and ensuree all rows
    are the same length.

    Args:
        text (str): raw text in string format

    Returns:
        io.StringIO: String buffer that can be read by pandas
    """

    length = 0
    for line in text.split("\n"):
        length = max(len(line.split(",")), length)

    lines = list()
    for line in text.split("\n"):
        parsed_line = line.split(",")
        if len(parsed_line) < 18:  # changed this from 20 to 18 to make sure all adeck forecast were being captured
            continue
        while len(parsed_line) < length:
            parsed_line.extend([""])
        lines.append(",".join(parsed_line))
    return StringIO("\n".join(lines))


def contains_date(file_path, date_time, date_col=2, sep=","):
    """Parse an ATCF bdeck or adeck file to see if the provided date_time is
    in it.

    Args:
        file_path (str, pathlib.Path): path to the adeck or bdeck file
        date_time (str, datetime): date_time to check for
        date_col (int, optional): Column where the date_times are. Defaults to 2.
        sep (str, optional): Seperator used in the file. Defaults to ','.

    Returns:
        bool : True if date_time is in the file. False otherwise
    """
    if isinstance(date_time, datetime):
        date_str = date_time.strftime("%Y%m%d%H")
    else:
        date_str = date_time

    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if file_path.name.endswith(".gz"):
        text = compressed_atcf_to_text(file_path)
    else:
        with open(file_path, "r") as t:
            text = t.read()

    text = text.strip()
    lines = text.split("\n")
    dates = {line.split(sep)[date_col].strip() for line in lines}
    if date_str in dates:
        return True
    else:
        return False


def toStormDict(path):
    if not isinstance(path, pathlib.Path):
        path = Path(path)


    basin = path.name[1:3]
    # determine whic organization the data is coming from (to be used in the name for invests)
    if basin.lower() in ['al', 'ep', 'cp']:
        org = 'NHC'
    else:
        org = 'JTWC'

    df = parse_bDeck(path)
    if df.empty:
        return None

    # determine the "strongest" storm type the storm has experienced so we can use
    # that in the storm name
    storm_type = get_storm_type(df.VMAX.max(), df.BASIN.values[0]) 

    name = df.STORMNAME.values[-1].title()
    nhc_number = df.SNUM.values[-1]
    subregion = df.SUBREGION.values[-1]
    if nhc_number >= 70:
        name = f"{org.upper()}-{nhc_number:02d}{subregion}"
    else:
        name = f"{storm_type}-{name}"

    start_date = df.DATETIME.min()
    end_date = df.DATETIME.max()
    season = start_date.year
    nhc_number = nhc_number
    region = df.BASIN.values[0]
    nhc_id = f"{region}{nhc_number:02d}{season}".upper()
    name = name
    start_lat = val.validate_latitude(df.LAT.values[0], raise_on_fail=True)
    start_lon = val.validate_longitude(df.LON.values[0], raise_on_fail=True)
    storm_dict = dict(
        nhc_number=int(nhc_number),
        nhc_id=nhc_id,
        season=int(season),
        start_date=start_date,
        end_date=end_date,
        name=name,
        start_lat=float(start_lat),
        start_lon=float(start_lon),
    )
    return storm_dict


def observationDictFromDataFrame(ob, storm_id=None):
    """Create a new Observation instance from a BDeck observation set

    Args:
            ob (pandas.DataFrame): A DataFrame of BDeck observations
            storm_id (int): Storm id that the observation belongs to

    Returns:
            tables.Observation
    """
    if len(ob.DATETIME.unique()) != 1:
        raise ValueError(
            "Observation rows may only contain a single unique datetime. Received {len(ob.DATETIME.unique())} unique datetimes"
        )

    date_time = ob.DATETIME.unique()[0]

    r34 = getRadialValues(ob, 34)
    r50 = getRadialValues(ob, 50)
    r64 = getRadialValues(ob, 64)

    observation = dict(
        storm_id=storm_id,
        datetime_utc=pd.Timestamp(date_time),
        latitude=val.validate_latitude(ob.LAT.values[0], raise_on_fail=True),
        longitude=val.validate_longitude(ob.LON.values[0], raise_on_fail=True),
        intensity_kts=val.validate_velocity(ob.VMAX.values[0], raise_on_fail=True),
        mslp_mb=val.validate_pressure(ob.MSLP.values[0]),
        r34_ne=r34.get("NE"),
        r34_se=r34.get("SE"),
        r34_sw=r34.get("SW"),
        r34_nw=r34.get("NW"),
        r50_ne=r50.get("NE"),
        r50_se=r50.get("SE"),
        r50_sw=r50.get("SW"),
        r50_nw=r50.get("NW"),
        r64_ne=r64.get("NE"),
        r64_se=r64.get("SE"),
        r64_sw=r64.get("SW"),
        r64_nw=r64.get("NW"),
        pouter_mb=val.validate_pressure(ob.POUTER.values[0]),
        router_nmi=val.validate_distance(ob.ROCI.values[0]),
        rmw_nmi=val.validate_distance(ob.RMW.values[0]),
    )

    return observation


def stepFromDataFrame(df, hour, track_id):

    r34 = getRadialValues(df, 34)
    r50 = getRadialValues(df, 50)
    r64 = getRadialValues(df, 64)

    step = dict(
        track_id=track_id,
        hour=val.validate_forecast_step(hour),
        latitude=val.validate_latitude(df.LAT.values[0], raise_on_fail=True),
        longitude=val.validate_longitude(df.LON.values[0], raise_on_fail=True),
        intensity_kts=val.validate_velocity(df.VMAX.values[0], raise_on_fail=True),
        mslp_mb=val.validate_pressure(df.MSLP.values[0]),
        r34_ne=r34.get("NE"),
        r34_se=r34.get("SE"),
        r34_sw=r34.get("SW"),
        r34_nw=r34.get("NW"),
        r50_ne=r50.get("NE"),
        r50_se=r50.get("SE"),
        r50_sw=r50.get("SW"),
        r50_nw=r50.get("NW"),
        r64_ne=r64.get("NE"),
        r64_se=r64.get("SE"),
        r64_sw=r64.get("SW"),
        r64_nw=r64.get("NW"),
    )

    return step


def getRadialValues(df, rad):

    assert len(df) <= 3, f"DataFrame has {len(df)} rows. Can't have more than 3"

    quads = ["NE", "SE", "SW", "NW"]
    r = df.loc[df.RAD == rad]
    if r.empty:
        return {quad: None for quad in quads}
    else:
        return {quad: int(val.validate_distance(r[quad].values[0])) for quad in quads}
