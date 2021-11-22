import pandas as pd
import pymysql
from datetime import datetime,timedelta

conn = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_cursor = conn.cursor(pymysql.cursors.DictCursor)

# create_sql = \
# """
# create table filerecorded (
# 	id int not null primary key auto_increment,
#     upload_date timestamp not null,
#     begin_date timestamp not null,
#     end_date timestamp not null,
#     file_path varchar(256) not null,
#     file_basename varchar(256) not null,
#     location_id int,
#     client_id int,
#     foreign key (location_id) references location (id),
#     foreign key (client_id) references sa_api_client (id)
# );
# """

## file list quary

sql = \
"""
select *
from sa_api_filerecorded
where begin_date >= '2017-01-01' and end_date >= '2017-01-01' and begin_date < end_date;
"""
test_cursor.execute(sql)
file_list = test_cursor.fetchall()
pd_file_list = pd.DataFrame(file_list)
pd_file_list_key = pd_file_list.keys()


## file time preprocessing

pd_file_list['upload_date'] = pd_file_list['upload_date'] + timedelta(hours = 9)
pd_file_list['begin_date'] = pd_file_list['begin_date'] + timedelta(hours = 9)
pd_file_list['end_date'] = pd_file_list['end_date'] + timedelta(hours = 9)
pd_file_list = pd_file_list[(pd_file_list['bed_id'].isnull() != True) & (pd_file_list['client_id'].isnull() != True)].reset_index(drop=False)
pd_file_list['bed_id'] = pd_file_list['bed_id'].astype(int)
pd_file_list['client_id'] = pd_file_list['client_id'].astype(int)



## insert

insert_sql = """insert into filerecorded (upload_date,begin_date,end_date,basename,location_id,client_id)
values ("""

for i,j,k,l,m,n in zip(pd_file_list['upload_date'],pd_file_list['begin_date'],pd_file_list['end_date'],pd_file_list['file_basename'],pd_file_list['bed_id'],pd_file_list['client_id']) :
    bed_sql = f"""select id from location where name = (select name from sa_api_bed where id = {m});"""
    test_cursor.execute(bed_sql)
    bed_info=test_cursor.fetchall()
    value_sql = f"""'{i}','{j}','{k}','{l}',{bed_info[0]['id']},{n} ) """
    test_cursor.execute(insert_sql+value_sql)
    # break
# print(insert_sql,value_sql)
conn.commit()


for i in range(50000) :
    delete_sql = f"""delete from filerecorded where id={i};"""
    test_cursor.execute(delete_sql)