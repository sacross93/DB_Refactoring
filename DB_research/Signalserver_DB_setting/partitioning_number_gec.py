import pymysql

conn = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_cursor = conn.cursor(pymysql.cursors.DictCursor)

sql = \
"""
alter table number_gec_shar partition by range (id)(
    partition id_1 values less than (50000000),
    partition id_2 values less than (50000000+50000000),
    partition id_3 values less than (50000000+50000000+50000000),
    partition id_4 values less than (50000000+50000000+50000000+50000000),
    partition id_5 values less than (50000000+50000000+50000000+50000000+50000000)
    );
"""