import pymysql
import numpy as np
import pandas as pd
import time
import datetime

def dbIn() :
    result = pymysql.connect(
        user='wlsdud1512',
        passwd='wlsdud1512',
        host='192.168.44.106',
        db='sa_server'
    )
    cursor = result.cursor(pymysql.cursors.DictCursor)

    return result , cursor

db,cursor=dbIn()


# select count(ABP_SBP),count(ABP_DBP),count(ABP_MBP)
# from number_gec
# where dt between '2019-04-01' and '2020-04-01';

start_list=[]
end_list=[]
during_list=[]

start_time=datetime.datetime(year=2020,month=1,day=1)

for i in range(2,9999999999) :

    during_time = time.time()
    end_time=start_time + datetime.timedelta(days=i)
    print(start_time," between ", end_time)
    sql="""select count(ABP_SBP),count(ABP_DBP),count(ABP_MBP) from number_gec where dt between %s and %s ;"""
    cursor.execute(sql,(start_time,end_time))
    during_end_time=time.time() - during_time
    print(time.time()-during_time)
    start_list.append(start_time)
    end_list.append(end_time)
    during_list.append(during_end_time)

    excel_file=pd.DataFrame(
        {
            'start time' : start_list,
            'end time' : end_list,
            'time' : during_list
        }
    )

    if end_time > datetime.datetime(year=2021,month=5,day=1) :
        break

    excel_file.to_excel('/mnt/jy_excel/DB_list2.xlsx')


