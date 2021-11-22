import pandas as pd
import vr_reader_jy as vr
import matplotlib.pyplot as plt
import numpy as np
import pymysql
from multiprocessing import *

operation_file = pd.read_excel('/home/projects/pcg_transform/Meeting/jy/operation_file_ver3.xlsx',engine='openpyxl')

print(len(operation_file))
operation_file=operation_file[(operation_file['ABP'] != 'X') & (operation_file['ECG'] != 'X') & (operation_file['PLETH'] != 'X')].reset_index(drop=True)
print(len(operation_file))
print(f"ABP,ECG,PLETH 모두 있는 수술 건수 : {len(operation_file['operation_id'].unique())}")

def npz_extract (i) :
    main_conn = pymysql.connect(
        user='wlsdud1512',
        passwd='wlsdud1512',
        host='192.168.44.106',
        db='sa_server',
        charset='utf8'
    )
    main_cursor = main_conn.cursor(pymysql.cursors.DictCursor)
    temp = operation_file[operation_file['operation_id'] == i]
    print(f"operation_id : {i}")
    # if len(temp) <= 1 :
    #     continue
    OP_ABP_data = []
    OP_ABP_time = []
    OP_ECG_data = []
    OP_ECG_time = []
    OP_PLETH_data = []
    OP_PLETH_time = []
    for j,k,l,n in zip(temp['file_name'],temp['ABP'],temp['location_name'],temp['file_id']) :
        temp_file = j.split('_')
        address = f"/srv/data/vital_amc/{temp_file[0]}/{temp_file[1]}/{j}"
        vital_file = vr.VitalFile(address)
        trks_list = vital_file.find_trks()
        for trk in trks_list :
            if ('INTELLIVUE/ABP' in trk.upper() and trk.endswith('ABP')) or ('GE/Carescape/IBP' in trk or 'Bx50/IBP') in trk :
                temp_trk = trk.split('/')
                ABP_time, ABP_data = vital_file.get_samples(temp_trk[-1])
                OP_ABP_time.extend(ABP_time)
                OP_ABP_data.extend(ABP_data)
                # print(f"BP_name : {trk}, length : {len(OP_ABP_time)}")
                # plt.figure(figsize=(20,10))
                # plt.plot(OP_ABP_time,OP_ABP_data)
                # plt.show()
                break
            elif 'DI-' in trk and 'ART' in trk :
                temp_trk = trk.split('/')
                ABP_time, ABP_data = vital_file.get_samples(temp_trk[-1])
                # print(f"BP_name : {trk}, length : {len(ABP_time)}")
                break
            else :
                ABP_time = []
                ABP_data = []
        ECG_sql = f"""
        select *
        from sa_api_waveinfofile
        where channel_name like '%ECG%' and record_id = {n};
        """
        main_cursor.execute(ECG_sql)
        ECG_name = pd.DataFrame(main_cursor.fetchall())
        ECG_time, ECG_data = vital_file.get_samples(ECG_name['channel_name'][0])
        OP_ECG_data.extend(ECG_data)
        OP_ECG_time.extend(ECG_time)
        PLETH_sql = f"""
        select *
        from sa_api_waveinfofile
        where channel_name like '%PLETH%' and record_id = {n};
        """
        main_cursor.execute(PLETH_sql)
        PLETH_name = pd.DataFrame(main_cursor.fetchall())
        PLETH_time, PLETH_data = vital_file.get_samples(PLETH_name['channel_name'][0])
        OP_PLETH_time.extend(PLETH_time)
        OP_PLETH_data.extend(PLETH_data)

        # print(f"operation_id : {i}")
    print(f"ECG : {len(OP_ECG_time)}, PLETH : {len(OP_PLETH_data)}, ABP : {len(OP_ABP_time)}")
    print(f"date : {temp_file[1]}, location_name : {l}")
    if len(OP_ECG_time) >= 1000 or len(OP_PLETH_time) >= 1000 or len(OP_ABP_time) >= 1000 :
        np.savez(f"/home/projects/pcg_transform/Meeting/jy/MIRL/{i}_{temp_file[1]}_{l}.npz",ABP_data = OP_ABP_data, ABP_time = OP_ABP_time, ECG_time = OP_ECG_time, ECG_data = OP_ECG_data, PLETH_data = OP_PLETH_data, PLETH_time = OP_PLETH_time)


with Pool(6) as p :
    p.map(npz_extract,operation_file['operation_id'].unique())