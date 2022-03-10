import pymysql
import pandas as pd
import os
import sys
import warnings

def test_dbIn() :
    conn = pymysql.connect(
        user='wlsdud022',
        passwd='wlsdud022',
        host='192.168.134.193',
        db='SignalHouse',
        charset='utf8',
        port=3307
    )
    test_DB_cursor = conn.cursor(pymysql.cursors.DictCursor)

    return conn, test_DB_cursor

def dir_empty(dir_path):
    return not any([True for _ in os.scandir(dir_path)])

mysql,cursor = test_dbIn()

# address = "/home/projects/pcg_transform/Operation_data/operation_list/"
address = "/srv/operation_storage/"
file_list = os.listdir(address)

for file in file_list :

    try : operation_list = pd.read_excel(address+file,engine='openpyxl')
    except : continue

    insert_sql = \
        """
        insert ignore into operation (start_date,end_date,location_id) 
        """

    for j, k, l in zip(operation_list['op_room'], operation_list['start_date'], operation_list['end_date']):
        sql = f"""select id from location where name = '{j}';"""
        if cursor.execute(sql) != 0:
            temp_bed_id = cursor.fetchall()
            values_sql = f"""values ('{k}','{l}',{temp_bed_id[0]['id']})"""
            cursor.execute(insert_sql + values_sql)
    mysql.commit()
