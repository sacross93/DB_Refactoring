import pymysql
import pandas as pd
import jyLibrary as jy
import vr_reader_jy as vr
from datetime import timedelta

#### DB connect
def test_dbIn() :
    conn = pymysql.connect(
        user='wlsdud022',
        passwd='wlsdud022',
        host='192.168.134.193',
        db='SignalHouse',
        charset='utf8'
    )
    test_DB_cursor = conn.cursor(pymysql.cursors.DictCursor)

    return conn, test_DB_cursor

def main_dbIn() :
    main_conn = pymysql.connect(
        user='wlsdud1512',
        passwd='wlsdud1512',
        host='192.168.44.106',
        db='sa_server',
        charset='utf8'
    )
    main_cursor = main_conn.cursor(pymysql.cursors.DictCursor)

    return main_conn, main_cursor

conn193,cursor193 = test_dbIn()
conn106,cursor106 = main_dbIn()

operation_file = pd.read_excel('/home/projects/pcg_transform/Meeting/jy/operation_file_ver2.xlsx',engine='openpyxl')
operation_file.keys()
op_id = operation_file['operation_id'].unique()

for i in op_id :
    temp_info = operation_file[operation_file['operation_id']==i]
    break
    # for j in temp_info :

## test

for j in temp_info['file_name'] :
    sql = f"""
    select *
    from sa_api_waveinfofile as wave
    join sa_api_filerecorded as file
    on wave.record_id = file.id
    where file.file_basename = '{j}' and (wave.channel_name like 'IBP%' or wave.channel_name like 'ABP%' or wave.channel_name like 'ART%');"""
    print(cursor106.execute(sql))
    break

    # if wavedata == 'ABP' :
    #     sql += f"""'IBP%' or wave.channel_name like 'ABP%' or wave.channel_name like 'ART%');"""
    # else :
    #     sql += f"""'%{wavedata}%');"""

    address = j.split('_')
    address = f"/srv/data/vital_amc/{address[0]}/{address[1]}/{j}"
    vital_file = vr.VitalFile(address)
    jy.dataSearch()

