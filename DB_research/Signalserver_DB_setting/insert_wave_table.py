import os.path
import pymysql
import pandas as pd
import datetime
import vr_reader_fix as vr
import sys

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

sql = \
"""
select *
from sa_api_waveinfofile;
"""

test_DB_cursor.execute(sql)
waveresult=pd.DataFrame(test_DB_cursor.fetchall())
wave_key = waveresult.keys()

file_sql = \
    f"""select file.begin_date,file.end_date,file.file_basename,bed.name,wave.channel_name
    from sa_api_filerecorded as file
    join sa_api_bed as bed
    on bed.id = file.bed_id
    join sa_api_waveinfofile as wave
    on wave.record_id = file.id
    where file.id in ("""

for i in waveresult['record_id'] :
    file_sql += f""" '{i}',"""
file_sql = file_sql[:-1]+");"

test_DB_cursor.execute(file_sql)
file_info = pd.DataFrame(test_DB_cursor.fetchall())
file_info_keys = file_info.keys()

## base preprocessing end

## insert wave table preprocessing

file_path = "/srv/data/vital_amc/"

for i,j,k in zip(file_info['file_basename'],file_info['name'],file_info['channel_name']) :
    temp_cal = i.split('_')[1]
    temp_addr = file_path+j+'/'+temp_cal+'/'+i
    temp_size = os.path.getsize(temp_addr)
    if temp_size > 52428800 :
        vr_file = vr.VitalFile(temp_addr)
        break
k
vr_file.get_samples('ABP')