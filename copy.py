import shutil
import pytz
from dateutil import parser
from datetime import datetime, timedelta
import os

def _is_in_range(tz, date):
    dt = parser.parse(date)
    dt = dt.astimezone(pytz.timezone(tz))
    st = _get_time('2019-4-11', tz)
    et = _get_time('2019-5-20', tz) + timedelta(hours = 3)
    return dt >= st and dt <= et

def _get_time(ti, tz):
    time = datetime.strptime(ti, '%Y-%m-%d')
    return pytz.timezone(tz).localize(time)

files = sorted(os.listdir('tweethouse'))
fi = [f for f in files if _is_in_range('Asia/Kolkata', f)]
fi = [(os.path.join('tweethouse', f),
       os.path.join('Dropbox', '01_India_Twitter_Data', 'raw-data'))
      for f in fi]

for src,dst in fi:
    shutil.move(src, dst)
