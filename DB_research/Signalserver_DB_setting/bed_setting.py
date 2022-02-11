import pymysql

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

# create_sql = \
# """
# create table location (
# 	id int auto_increment primary key not null,
#     name varchar(16) not null,
#     type int not null
# );
# """
# test_cursor.execute(create_sql)
# conn.commit()

sql = """select * from sa_api_bed;"""
DB_cursor.execute(sql)
bed_info = DB_cursor.fetchall()

bed_name = []
bed_type = []
for i in bed_info :
    bed_name.append(i['name'])
    bed_type.append(i['bed_type'])

for i,j in zip(bed_name,bed_type) :
    insert_sql = \
    f"""
    insert into location (name,type)
        values ('{i}',{j});
    """
    test_cursor.execute(insert_sql)
conn.commit()
#a