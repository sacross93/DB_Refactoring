import pymysql
import pandas as pd
import os
import sys

def test_dbIn() :
    conn = pymysql.connect(
        user='wlsdud022',
        passwd='wlsdud022',
        host='192.168.134.193',
    )
    test_DB_cursor = conn.cursor(pymysql.cursors.DictCursor)

    return conn, test_DB_cursor

def dir_empty(dir_path):
    return not any([True for _ in os.scandir(dir_path)])

mysql,cursor = test_dbIn()

oper_path = "/home/projects/pcg_transform/Operation_data/operation_list/"
save_path = "/srv/data/operation_storage/"

if dir_empty(oper_path) == True :
    sys.exit()

oper_excel_list = os.listdir(oper_path)

for i in oper_excel_list :
    operation_list = pd.read_excel(oper_path+i,engine='openpyxl')
    operation_list = operation_list.drop(['RP_마취통증의학과-021.마취통증의학과_수술 실적 보고서','Unnamed: 2'],axis='columns')
    operation_list.columns = ['op_room', 'start_date', 'end_date']
    break



## test

oper_path = "/home/projects/pcg_transform/Operation_data/operation_list/"
save_path = "/srv/data/operation_storage/"
oper_excel_list = os.listdir(oper_path)
oper_name = 'RP_마취통증의학과-021_마취통증의학과_수술 실적 보고서.xlsx'

operation_list = pd.read_excel(oper_path+oper_name,engine='openpyxl')
operation_list = operation_list.drop(['RP_마취통증의학과-021.마취통증의학과_수술 실적 보고서','Unnamed: 2'],axis='columns')
operation_list.columns = ['op_room','start_date','end_date']
operation_list['op_room'] = operation_list['op_room'].fillna(method='pad')
operation_list = operation_list[(operation_list['start_date'].isnull() != True) & (operation_list['end_date'].isnull() != True) & (operation_list['op_room'] != '수술실코드')].reset_index(drop=False)
operation_list.to_excel(save_path+"2017to2021_operation_list.xlsx",index=False)