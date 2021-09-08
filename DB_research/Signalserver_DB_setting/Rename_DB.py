import pymysql
import pandas as pd

mysql_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='sa_server',
    charset='utf8'
)
test_cursor = mysql_db.cursor(pymysql.cursors.DictCursor)

sql = \
"""
show tables;
"""
test_cursor.execute(sql)
table_list = pd.DataFrame(test_cursor.fetchall())



for i in table_list['Tables_in_sa_server'] :
    rename_sql = \
        f"""
    rename table sa_server.{i} to SignalHouse.{i};
    """
    test_cursor.execute(rename_sql)
mysql_db.commit()