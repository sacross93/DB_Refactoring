import pymysql

mysql_db = pymysql.connect(
    user='wlsdud1512',
    passwd='wlsdud1512',
    host='192.168.44.106',
    db='sa_server',
    charset='utf8'
)
DB_cursor=mysql_db.cursor(pymysql.cursors.DictCursor)

conn = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='test_sa_server',
    charset='utf8'
)
test_cursor = conn.cursor(pymysql.cursors.DictCursor)