import pymysql
import os
import pandas as pd

mysql_db = pymysql.connect(
    user='wlsdud1512',
    passwd='wlsdud1512',
    host='192.168.44.106',
    db='sa_server',
    charset='utf8'
)
DB_cursor=mysql_db.cursor(pymysql.cursors.DictCursor)

conn = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_cursor = conn.cursor(pymysql.cursors.DictCursor)

create_sql = \
"""
create table operation (
	id int auto_increment primary key not null,
    start_date timestamp,
    end_date timestamp,
    location_id int not null,
    foreign key(location_id) references location(id),
    index time_idx (start_date,end_date)
);
"""
test_cursor.execute(create_sql)
conn.commit()

op_path = "/srv/data/operation_storage/"
op_name = os.listdir(op_path)
op_list = pd.read_excel(op_path+op_name[0])
op_list['op_room']=op_list['op_room'].str.slice_replace(start=1,stop=1,repl='-')
op_list = op_list[ op_list['start_date'] != '_']

sql = """select * from location;"""
test_cursor.execute(sql)
bed_info = pd.DataFrame(test_cursor.fetchall())

insert_sql = \
"""
insert into operation (start_date,end_date,location_id) 
"""

for i,j,k in zip(op_list['op_room'],op_list['start_date'],op_list['end_date']) :
    sql = f"""select id from location where name = '{i}';"""
    if test_cursor.execute(sql) != 0 :
        temp_bed_id=test_cursor.fetchall()
        values_sql = f"""values ('{j}','{k}',{temp_bed_id[0]['id']})"""
        # print(i,j,k)
        test_cursor.execute(insert_sql + values_sql)
conn.commit()




## test
# op_list['op_room'].str.findall(pat='[0-9]')
# op_list['op_room'].str.get(i=0)
# op_list['op_room']=op_list['op_room'].str.slice_replace(start=1,stop=1,repl='-')
# op_list['op_room']
# insert_sql+values_sql
# test_cursor.execute(insert_sql+values_sql)
# conn.commit()
# conn.rollback()