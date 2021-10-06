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

test_sql = \
"""
select *
from number_gec_shar as gec_shar, location
where gec_shar.location_id = location.id and location.name = 'D-06' and dt >= '2021-01-01' and dt < '2021-03-01'
"""

test_DB_cursor.execute(test_sql)
gec_val = pd.DataFrame(test_DB_cursor.fetchall())




test_sql = \
"""
select *
from number_gec as gec_shar, sa_api_bed as bed
where gec_shar.record_id in (select id from sa_api_filerecorded where bed_id in (select id from sa_api_bed where name = 'D-06')) and dt >= '2021-01-01' and dt < '2021-03-01'
"""

test_DB_cursor.execute(test_sql)
gec_val = pd.DataFrame(test_DB_cursor.fetchall())