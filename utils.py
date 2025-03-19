from datetime import datetime, timedelta


# Convert period to milliseconds
def period_to_milliseconds(period):
    num = int(period[:-1])
    unit = period[-1]
    
    if unit == "d":
        return num * 24 * 60 * 60 * 1000
    elif unit == "h":
        return num * 60 * 60 * 1000
    elif unit == "m":
        return num * 60 * 1000
    else:
        raise ValueError("Invalid period format, use e.g., '365d', '48h'")
    

def period_to_timedelta(period):
    num = int(period[:-1])
    unit = period[-1]
    
    if unit == "d":
        return timedelta(days=num)
    elif unit == "h":
        return timedelta(hours=num)
    elif unit == "m":
        return timedelta(minutes=num)
    else:
        raise ValueError("Invalid period format, use e.g., '365d', '48h', '30m'")