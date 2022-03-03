import pymysql

conn = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    port=3307
)
create_cursor = conn.cursor()

create_cursor.execute("create database SignalHouse;")

conn.commit()
