import datetime as dt

def generate_time():
    time = dt.datetime.now()
    time = f"{time.day}{time.month}{time.year}-{time.hour}{time.minute}{time.second}"
    return time