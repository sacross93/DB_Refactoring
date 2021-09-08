import pymysql
import pandas as pd

mysql_db = pymysql.connect(
    user='wlsdud1512',
    passwd='wlsdud1512',
    host='192.168.44.106',
    db='sa_server',
    charset='utf8'
)
DB_cursor=mysql_db.cursor(pymysql.cursors.DictCursor)

sql = \
"""
select *
from sa_api_waveinfofile as wave
join sa_api_device as device
on wave.device_id = device.id
where device_id =3;
"""

DB_cursor.execute(sql)
a=pd.DataFrame(DB_cursor.fetchall())
DB_cursor.execute(sql)
prm_wave_list = pd.DataFrame(DB_cursor.fetchall())
prm_col = prm_wave_list.columns
