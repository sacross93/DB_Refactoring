import pymysql
import pandas as pd
from datetime import datetime
import re

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



cnt=0
for i in file_list['id'] :
    cnt += 1
    if cnt < 29602 :
        continue
    gec_data = \
    f"""
    select *
    from number_gec
    where record_id = {i}
    """
    test_DB_cursor.execute(gec_data)
    gec_raw = test_DB_cursor.fetchall()
    gec_list=pd.DataFrame(gec_raw)
    gec_keys=gec_list.keys()
    insert_sql = \
        """
        insert into number_gec_shar"""
    value_sql = """("""
    # break
    for j in gec_keys :
        if j != 'record_id' and j != 'operation_id' and j != 'id' :
            value_sql += f"{j},"
    value_sql += "location_id)"
    # break
    # values_sql = """"""
    for k in gec_raw :
        # values_sql += """ values("""
        values_sql = """ values("""
        for n in gec_keys :
            if n == 'dt' :
                # values_sql += f"""{int(datetime.timestamp(k[n]))},"""
                values_sql += f"""'{k[n]}',"""
            elif n != 'record_id' and n != 'operation_id' and n != 'id' :
                if k[n] == None or k[n] == 'None' :
                    values_sql += """null,"""
                else :
                    values_sql += f"""{k[n]},"""
        bed_name_sql = \
        f"""
        select *
        from location
        where name in (
        select bed.name
        from sa_api_filerecorded as file
        join sa_api_bed as bed
        on bed.id = file.bed_id
        where file.id = {i} 
        );
        """
        test_DB_cursor.execute(bed_name_sql)
        location=pd.DataFrame(test_DB_cursor.fetchall())
        values_sql += f"""{location['id'][0]})
        """
        test_DB_cursor.execute(insert_sql + value_sql + values_sql)
    test_db.commit()
    # values_sql = values_sql[:-10]
    # break
    # test_DB_cursor.execute(insert_sql+value_sql+values_sql)
    # test_db.commit()
    # print(insert_sql+value_sql+values_sql[:20])
    # cnt += 1
    # if cnt == 2 :
    #     break
    # break



for x in range(len(file_list['id'])) :
    if file_list['id'][x] == 52453 :
        print(x)
        break


#test
value_sql
test = value_sql.split(',')
len(test)
test
test = values_sql.split(',')
len(test)

print(insert_sql+value_sql+values_sql[:20])
for k in gec_keys :
    continue

values_sql[:100]
values_sql
int(datetime.timestamp(k['dt']))
insert_sql
value_sql
test=values_sql.split('\n')
test[0]

test_db.rollback()