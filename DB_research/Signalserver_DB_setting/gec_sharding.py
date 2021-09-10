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

# sql = \
# """
# desc number_gec;
# """
# test_DB_cursor.execute(sql)
# desc_gec=pd.DataFrame(test_DB_cursor.fetchall())
#
# create_sql = """create table number_gec_shar(
#     """
#
# for i,j in zip(desc_gec['Field'],desc_gec['Type']) :
#     if i == 'record_id' or i == 'operation_id' :
#         continue
#     if i == 'dt' :
#         create_sql +=  f"""{i} timestamp"""
#     else :
#         create_sql += f"""{i} {j}"""
#     if i == 'id' :
#         create_sql += " auto_increment"
#     create_sql += """,
#     """
# create_sql += """location_id int,
#     primary key(id),
#     index time_idx (dt),
#     foreign key (location_id) references location(id) """
# create_sql += """
# );"""
# print(create_sql)
# test_DB_cursor.execute(create_sql)
# test_DB_cursor.fetchall()
# test_db.commit()

file_sql = \
"""
select distinct(file.id)
from sa_api_filerecorded as file
join number_gec as gec
on gec.record_id = file.id
"""
test_DB_cursor.execute(file_sql)
file_list = pd.DataFrame(test_DB_cursor.fetchall())

for i in file_list['id'] :
    gec_data = \
    f"""
    select *
    from number_gec
    where record_id = {i}
    """
    test_DB_cursor.execute(gec_data)
    gec_list=pd.DataFrame(test_DB_cursor.fetchall())
    gec_keys=gec_list.keys()
    for j in gec_keys :
        insert_sql = \
        """
        insert into table number_gec_shar
        """

for k in gec_keys :
