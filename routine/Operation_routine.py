import pymysql
import pandas as pd
import os
import sys

def test_dbIn() :
    conn = pymysql.connect(
        user='wlsdud022',
        passwd='wlsdud022',
        host='192.168.134.193',
        db='SignalHouse',
        charset='utf8'
    )
    test_DB_cursor = conn.cursor(pymysql.cursors.DictCursor)

    return conn, test_DB_cursor

def dir_empty(dir_path):
    return not any([True for _ in os.scandir(dir_path)])

mysql,cursor = test_dbIn()

oper_path = "/home/projects/pcg_transform/Operation_data/operation_list/"
save_path = "/srv/operation_storage/"

if dir_empty(oper_path) == True :
    sys.exit()

oper_excel_list = os.listdir(oper_path)

for i in oper_excel_list :
    # operation_list = pd.read_excel(oper_path+i,engine='openpyxl')
    operation_list = pd.read_excel(oper_path + i)
    try :
        operation_list = operation_list.drop(['RP_마취통증의학과-021.마취통증의학과_수술 실적 보고서','Unnamed: 2'],axis='columns')
    except :
        operation_list = operation_list.drop(['RP_마취통증의학과-021.수술 실적 보고서_마취통증의학과', 'Unnamed: 2'], axis='columns')
    operation_list.columns = ['op_room', 'start_date', 'end_date']
    # print(len(operation_list))
    operation_list['op_room'] = operation_list['op_room'].fillna(method='pad')
    operation_list = operation_list[(operation_list['start_date'].isnull() != True) & (operation_list['end_date'].isnull() != True) & (operation_list['op_room'] != '수술실코드')].reset_index(drop=False)
    # print(len(operation_list))
    operation_list['op_room'] = operation_list['op_room'].str.slice_replace(start=1,stop=1,repl='-')
    operation_list = operation_list[operation_list['start_date'] != '_']
    # print(len(operation_list))

    operation_list['start_date'] = pd.to_datetime(operation_list['start_date'])
    operation_list['end_date'] = pd.to_datetime(operation_list['end_date'])

    lastTempDate = operation_list['start_date'][len(operation_list['start_date']) - 1].strftime("%Y-%m-%d")
    firstTempDate = operation_list['start_date'][0].strftime("%Y-%m-%d")

    oper_name = firstTempDate+"_to_"+lastTempDate

    operation_list.to_excel(save_path + oper_name + ".xlsx", index=False)

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


for i in oper_excel_list :
    os.remove(oper_path+i)


## test
