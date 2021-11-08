import pymysql
import pandas as pd
from datetime import timedelta,datetime
from tqdm import tqdm

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

mysql_db = pymysql.connect(
    user='wlsdud1512',
    passwd='wlsdud1512',
    host='192.168.44.106',
    db='sa_server',
    charset='utf8'
)
DB_cursor=mysql_db.cursor(pymysql.cursors.DictCursor)

operation_sql = """
select *
from operation
join location
on location.id = operation.location_id;
"""
test_DB_cursor.execute(operation_sql)
operation_list = pd.DataFrame(test_DB_cursor.fetchall())
operation_list.keys()

# operation_list_2021 = operation_list[operation_list['start_date'] >= '2021-01-01']
# operation_list_2021['start_date'] = pd.DatetimeIndex(operation_list_2021['start_date']) - timedelta(hours=9)
# operation_list_2021['end_date'] = pd.DatetimeIndex(operation_list_2021['end_date']) - timedelta(hours=9)
operation_list['start_date'] = pd.DatetimeIndex(operation_list['start_date']) - timedelta(hours=9)
operation_list['end_date'] = pd.DatetimeIndex(operation_list['end_date']) - timedelta(hours=9)

count=0
cnt=0
file_basename=[]
op_date = []
rosette = []
for start,end,bed_name in zip(operation_list['start_date'],operation_list['end_date'],operation_list['name']) :
    bed_sql = f"""
    select *
    from sa_api_bed
    where name = '{bed_name}';
    """
    DB_cursor.execute(bed_sql)
    bed_id = DB_cursor.fetchall()

    sql = f"""
    select *
    from sa_api_filerecorded
    where (end_date >= '2017-01-01' and begin_date >= '2017-01-01') and ( (begin_date > '{start}'  and end_date < '{end}') or (begin_date > '{start}' and end_date < '{end}') or (begin_date < '{start}' and end_date > '{end}') or (begin_date < '{start}' and end_date > '{end}') ) and bed_id = {bed_id[0]['id']} ;
    """
    a = DB_cursor.execute(sql)
    result = DB_cursor.fetchall()
    if a != 0 :
        count += 1
        if start >= datetime(year=2021,month=1,day=1) :
            cnt += 1
        temp_file=[]
        for i in result :
            temp_file.append(i['file_basename'])
        file_basename.append(temp_file)
        op_date.append(start)
        rosette.append(bed_name)
        # print(f"count : {count}명, 수술날짜 : {start}, 수술방 : {bed_name}")
excel_result = pd.DataFrame({'File_name' : file_basename, 'date' : op_date, 'rosette' : rosette})
excel_result.to_excel('/home/projects/pcg_transform/Meeting/jy/op.xlsx')
print(f"2017~2021 full : {count}, 2021 only : {cnt}")
