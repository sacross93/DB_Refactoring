from influxdb import InfluxDBClient
import pymysql
import jyLibrary as jy
import datetime
import copy
from multiprocessing import *
import parmap

influxCli = InfluxDBClient('localhost',8086,'root','root','testdb')

influxCli.create_database('testdb')

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

def insertDB(gec) :
    id = gec['id']
    dt = gec['dt']
    record_id=gec['record_id']
    del gec['id']
    del gec['dt']
    del gec['operation_id']
    del gec['record_id']
    tempGec=copy.deepcopy(gec)
    for key in tempGec.keys() :
    # for key in keys :
        if gec[key] == None :
            del gec[key]
            # print(key)
        # gec.pop(key,None)
        test = {
            "measurement" : "test_number_gec",
             "tags" : { "bed_id" : bed_id},
            "time" : dt,
            "fields" : gec
        }
        gecsList.append(test)
        # print(test)
    # print(dt,"insert...")
    temp=0
    while True :
        if len(gecsList) <= 10000 :
            influxCli.write_points(gecsList)
            break
        elif len(gecsList) > temp+10000 :
            influxCli.write_points(gecsList[temp:temp+10000])
        else :
            influxCli.write_points(gecsList[temp:-1])
            break
        temp+=10000


    # print(dt,"finish")

# result = influxCli.query("""select value from test_AUDIO""")
print("realstart..")


mysql_query = """select distinct * from sa_api_filerecorded where begin_date >= '2019-06-11 22:59:16'; """""
a = cursor.execute(mysql_query)
rids = cursor.fetchall()
print(rids[0]['begin_date'])
# mysql_query = """select * from number_gec where record_id = %s; """""
# a = cursor.execute(mysql_query, (rids[0]['id']))
# gecs = cursor.fetchall()

num_cores = cpu_count()
print(num_cores)

for rid in rids:
    print("quary start...")
    mysql_query="""select * from number_gec where record_id = %s; """""
    a= cursor.execute(mysql_query, (rid['id']))
    gecs=cursor.fetchall()
    print("quary end...")
    gecsList=[]
    bed_id = rid['bed_id']
    print(rid['begin_date'],"start...")
    with Pool(12) as p :
        p.map(insertDB,gecs)
    print(rid['begin_date'],"end...")

#   with Poll(12) as p :







# with Pool(12) as p:
#     p.map(insertDB,rids)