import os
import sys
from datetime import datetime
from pathlib import Path


def getJobId(script, timestamp):
    if not isinstance(script, Path):
        script =  Path(script)

    return f"{script.name.split('.')[0]}-{timestamp}"


def getStormType(wind_speed, region="AL"):
    r"""
    Retrieve the 2-character tropical cyclone type (e.g., "TD", "TS", "HU") given the wind speed and region of origin.

    Args:
        wind_speed (int): Integer denoting sustained wind speed in knots.
        region (str): Region short name. Default is 'AL'

    Returns:
        str: String denoting the tropical cyclone type.
    """

    if region in ["AL", "EP"]:
        if wind_speed < 34:
            return "TD"
        elif wind_speed < 63:
            return "TS"
        else:
            return "HU"
    elif region == "WP":
        if wind_speed < 34:
            return "TD"
        elif wind_speed < 63:
            return "TS"
        elif wind_speed < 130:
            return "TY"
        else:
            return "STY"
    elif region == "SH":
        if wind_speed < 63:
            return "TC"
        else:
            return "STC"
    elif region == "IO":
        if wind_speed < 28:
            return "DE"
        elif wind_speed < 34:
            return "DD"
        elif wind_speed < 48:
            return "CS"
        elif wind_speed < 64:
            return "SCS"
        elif wind_speed < 90:
            return "VSCS"
        elif wind_speed < 120:
            return "ESCS"
        else:
            return "SuCS"
    else:
        return "CY"

def get_logger_config(file_name=None, level="INFO"):
    handles = []
    if file_name:
        handles.append({
            "sink": f"/home/jmiller/logs/{file_name}",
            "format": "{time:YYYY-MM-DD HH:mm:ss} | {level: <10} | {name}:{function}:{line} | {message}",
            "backtrace": "True",
            "catch": "True",
            "level": level,
            "enqueue": "True",
            "rotation": "1 week",
            "retention": "1 month",
            "compression": "zip"
        })
    handles.append({
        "sink": sys.stdout,
        "format": "<g>{time:YYYY-MM-DD HH:mm:ss}</> | <lvl>{level: <10}</> | <c>{name}</>:<c>{function}</>:<c>{line}</> | <lvl>{message}</>",
        "backtrace": "True",
        "catch": "True",
        "level": "DEBUG",
        "enqueue": "True",
    })

    config = {"handlers": handles}

    return config
