import pymysql
import os
import pandas as pd

conn = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_cursor = conn.cursor(pymysql.cursors.DictCursor)

address = "/home/projects/pcg_transform/Meeting/jy/"
file_list = os.listdir(address)
vital_list = pd.read_excel(address+'operation_file_ver3.xlsx')