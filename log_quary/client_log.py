import pymysql

def main_dbIn() :
    result = pymysql.connect(
        user='wlsdud1512',
        passwd='wlsdud1512',
        host='192.168.44.106',
        db='sa_server'
    )
    cursor = result.cursor(pymysql.cursors.DictCursor)

    return result , cursor

def test_dbIn() :
    conn = pymysql.connect(
        user='wlsdud022',
        passwd='wlsdud022',
        host='192.168.134.193',
    )
    test_DB_cursor = conn.cursor(pymysql.cursors.DictCursor)

    return conn, test_DB_cursor

main_db,main_cursor=main_dbIn()
test_db,test_cursor=test_dbIn()

