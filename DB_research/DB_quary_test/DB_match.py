import pymysql
import numpy as np

mysql_db = pymysql.connect(
    user='wlsdud1512',
    passwd='wlsdud1512',
    host='192.168.44.106',
    db='sa_server',
    charset='utf8'
)
DB_cursor=mysql_db.cursor(pymysql.cursors.DictCursor)

sql = "desc number_gec;"
DB_cursor.execute(sql)
a=DB_cursor.fetchall()

for i in a :
    if i['Key'] == 'MUL' :
        print(i)