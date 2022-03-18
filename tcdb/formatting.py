"""
Useful functions for formatting __repr__ strings and just strings in general
"""


def pretty_print(x, numchars):
    """Given an object `x`, call `str(x)` and format the returned string so
    that it is numchars long, padding with trailing spaces or truncating with
    ellipses as necessary
    """
    s = maybe_truncate(x, numchars)
    return s + " " * max(numchars - len(s), 0)


def maybe_truncate(obj, maxlen=500):
    s = str(obj)
    if len(s) > maxlen:
        s = s[: (maxlen - 3)] + "..."
    return s


def format_date_time(d):
    return d.strftime("%Y-%m-%d %H:%M:%S")
