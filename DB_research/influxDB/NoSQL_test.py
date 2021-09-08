from influxdb import InfluxDBClient
import pymysql
import jyLibrary as jy
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

mysql_query = """select max(id) from number_piv; """
a=cursor.execute(mysql_query)
file_id = cursor.fetchall()




influxCli = InfluxDBClient('localhost',8086,'root','root','testdb')

influxCli.create_database('testdb')

# select * from number_piv join sa_api_filerecorded on record_id = sa_api_filerecorded.id where begin_date > '2020-01-01' and begin_date < '2020-03-31' and ABP_SBP is not null and file_basename like 'D-' ;
#
# select * from number_gec join sa_api_filerecorded on record_id = sa_api_filerecorded.id where begin_date > '2019-01-01' and ABP_SBP is not null and file_basename not like 'D-' ;
# select * from number_gec join sa_api_filerecorded on record_id = sa_api_filerecorded.id where begin_date > '2020-01-01' and begin_date < '2020-03-31';



# mysql_query = """select id from sa_api_filerecorded where begin_date > '2020-01-01' and begin_date < '2020-03-31' and file_basename like 'D-' """
mysql_query = """select id from sa_api_filerecorded where begin_date < '2020-03-31' and file_basename like 'D-'; """
a=cursor.execute(mysql_query)
file_id = cursor.fetchall()

mysql_query = """select * from sa_api_filerecorded where begin_date > '2019-06-01' and begin_date < '2019-06-31'; """
a=cursor.execute(mysql_query)
file_id = cursor.fetchall()


print(len(file_id))
print(file_id[0])


mysql_query

# result = influxCli.query("""  SELECT * FROM test_AUDIO where id = %s """,(file_id[0]['id']))
result = influxCli.query("""  select * from test_number_gec4 where time >= '2019-06-21' """)

result


