import pymysql
import pandas as pd

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

# create_sql = \
# """
# create table location_status_record
# (
#     id          int primary key auto_increment,
#     date        timestamp not null,
#     location_id int       not null,
#     status_id      int       not null,
#     foreign key (location_id) references location (id),
#     foreign key (status_id) references status (id),
#     index time_idx (date),
#     index status_idx (status_id)
# );
# """

sql = \
"""
select *
from sa_api_bed_status_record;
"""

test_DB_cursor.execute(sql)
status_list = pd.DataFrame(test_DB_cursor.fetchall())


for i,j,k in zip(status_list['date'],status_list['bed'],status_list['status']) :
    bed_sql = \
    f"""
    select id
    from location
    where name = '{j}';
    """
    test_DB_cursor.execute(bed_sql)
    bed_id=test_DB_cursor.fetchall()
    if 'reen' in k:
        status=1
    elif 'Blue' in k :
        status=2
    else :
        status=3
    insert_sql= \
    f"""
    insert into location_status_record (date,location_id,status)
    values ('{i}',{bed_id[0]['id']},{status})
    """
    test_DB_cursor.execute(insert_sql)