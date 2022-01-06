import os
import pandas as pd
from datetime import datetime
import pymysql

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

address = "/srv/project_data/operation_collection/"
file_list = os.listdir(address)

bad = pd.read_excel(address+file_list[0])
good = pd.read_excel(address+file_list[-2])
fall = pd.read_excel(address+file_list[2])


##3차 수집 성공 건수
cnt=0
for i in good['start'] :
    temp_false_time = i.date() + pd.offsets.Hour(17)
    temp_false_time2 = i.date() + pd.offsets.Hour(7)
    if i >= temp_false_time or i <= temp_false_time2:
        print(i)
        cnt += 1

##3차 수집 실패 건수
cnt=0
for i in bad['start'] :
    temp_false_time = i.date() + pd.offsets.Hour(17)
    temp_false_time2 = i.date() + pd.offsets.Hour(7)
    if i >= temp_false_time or i < temp_false_time2:
        print(i)
        cnt += 1
cnt


## 2차 수집 건수
cnt=0
for i in fall['op'] :
    temp = i.split(" ")
    temp_time = datetime.strptime(temp[0]+" "+temp[1],"%Y-%m-%d %H:%M:%S")
    temp_false_time = datetime.strptime(temp[0]+" 17:00:00","%Y-%m-%d %H:%M:%S")
    temp_false_time2 = datetime.strptime(temp[0] + " 08:00:00", "%Y-%m-%d %H:%M:%S")
    if temp_time >= temp_false_time or temp_time <= temp_false_time2:
        print(temp_time)
        cnt += 1

cnt

for i in fall['op'] :
    temp = i.split(" ")
    temp_time = datetime.strptime(temp[0]+" "+temp[1],"%Y-%m-%d %H:%M:%S")
    sql = f"""
    select *
    from operation
    where start_date = '{temp_time}';
    """
    print(sql)
    print(i)
    print(test_DB_cursor.execute(sql))
    print("")