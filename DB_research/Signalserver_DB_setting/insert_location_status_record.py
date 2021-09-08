import pymysql
import pandas as pd

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

sql = \
"""
select *
from sa_api_bed_status_record;
"""

test_DB_cursor.execute(sql)
status_record = test_DB_cursor.fetchall()

