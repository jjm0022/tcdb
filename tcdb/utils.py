import numpy as np
import json


def greatCircleDistance(lat1, lon1, lat2, lon2, units="nm"):

    lat1 = lat1 * (np.pi / 180)
    lat2 = lat2 * (np.pi / 180)
    lon1 = lon1 * (np.pi / 180)
    lon2 = lon2 * (np.pi / 180)

    dist = 2.0 * np.arcsin(
        np.sqrt((np.sin((lat1 - lat2) / 2)) ** 2 + np.cos(lat1) * np.cos(lat2) * (np.sin((lon1 - lon2) / 2) ** 2))
    )

    if units == "nm":
        dist = dist * (180.0 / np.pi) * 60
    elif units == "km":
        dist = dist * (180.0 / np.pi) * 60 * 1.852
    elif units == "mi":
        dist = dist * (180.0 / np.pi) * 60 * 1.15077945
    elif units == "degrees":
        dist = dist * (180.0 / np.pi)

    return dist

def get_storm_type(wind_speed,region="AL"):
    
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
    

def is_serializable(data):
    """Determine if a variable can be serialized. 

    Args:
        data (any): Variable to be serialized

    Returns:
        bool: True if the variable can be serialized
    """
    try:
        json.dumps(data)
        return True
    except (TypeError, OverflowError):
        # OverflowError is thrown when data contains a number which is too large for JSON to decode
        return False
