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

create_sql = \
"""
create table location_status_record(
    id int primary key auto_increment
    date timestatmp not null,
    location_id int not null,
    status int not null
"""