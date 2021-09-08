import pymysql
import numpy as np

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
    db='Test_SignalHouse',
    # db='test_sa_server',
    charset='utf8'
)
testDB_cursor = conn.cursor(pymysql.cursors.DictCursor)

calendar = np.arange(1,13)


## create table

sql = """desc number_gec"""
DB_cursor.execute(sql)
piv_info=DB_cursor.fetchall()

piv_col=[]

for i in piv_info :
    for j in i :
        print(i[j],j)
    print("")

for k in calendar :
    create_sql = """create table number_gec_"""+"2021"+str(k).zfill(2)+"""("""
    for i in piv_info :
        piv_col.append(i['Field'])
        for j in i :
            # print(i[j])
            if j == 'Field' :
                if i[j] != 'operation_id':
                    create_sql += f""" {i[j]} """
                else :
                    break
            elif j == 'Type' :
                if i[j] == 'datetime(6)' :
                    create_sql += """timestamp """
                    break
                if i[j] == 'float' :
                    create_sql += f"""{i[j]}(6,3)"""
                else :
                    create_sql += f"""{i[j]} """
            elif j == 'Null' :
                if i[j] == 'NO' :
                    create_sql += """not null """
            elif j == 'Key' :
                if i[j] == 'PRI' :
                    create_sql += """primary key """
                # elif i[j] == 'MUL' :
            elif j == 'Extra' :
                if i[j] == 'auto_increment' :
                    create_sql += f"""{i[j]} """
        if i[j] != 'operation_id':
            create_sql += ","

    create_sql += """location_id int(11) """
    create_sql += ");"
    print(create_sql)
    testDB_cursor.execute(create_sql)

conn.commit()

##
## alter foreign key

sql = """desc sa_api_filerecorded"""
DB_cursor.execute(sql)
file_info=DB_cursor.fetchall()

