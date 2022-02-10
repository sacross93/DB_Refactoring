import pymysql
import os
import pandas as pd
import datetime

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

address = "/srv/project_data/data_rate/"
file_list = os.listdir(address)

D_rate_df = pd.read_excel(address+"D-rosette_rate_12.xlsx")

for i in range(1,7) :
    op_sql = f"""
    select *
    from operation
    join location
    on location.id = operation.location_id
    where start_date >= '2021-12-01' and start_date < '2022-01-01' and location.name like '%D-0{i}%'
    order by start_date asc;
    """
    test_DB_cursor.execute(op_sql)
    temp = test_DB_cursor.fetchall()
    # print(len(temp))
    print(round(D_rate_df.groupby('rosette')['rosette'].count()[i-1]/len(temp),2))



op_sql = f"""
select *
from operation
join location
on location.id = operation.location_id
where start_date >= '2021-12-01' and start_date < '2022-01-01' and location.name like '%D-%'
order by start_date asc;
"""
test_DB_cursor.execute(op_sql)
temp = test_DB_cursor.fetchall()
len(temp)
D_rate_df.groupby('rosette')['rosette'].count()

##test
datetime.datetime.fromtimestamp(1628204400)
datetime.datetime.fromtimestamp(1628221500)
round(26/30,2)
round((80+100+80+84+73+97)/6)
47+28+44+52+35+29
round(235/284,2)
(48+16+53+58+44+23)/259
(51+20+43+49+38+23)/235
(43+26+43+63+47+28)/272
(50+27+63+60+48+32)/284