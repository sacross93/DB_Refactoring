import vr_reader_jy as vr
import jyLibrary as jy
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
from multiprocessing import *
import datetime
import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000

global error_list
error_list = {'vital_error':[],'None_data':[],'under_std':[],'under_op':[]}

def isNear(op_time_list,data_time) :
    result = []
    for op_time in op_time_list :
        result.append(abs(datetime.datetime.timestamp(op_time['start_date']) - data_time))
    try :
        tmp = min(result)
    except :
        return 99
    index = result.index(tmp)

    return index

def preprocessing_1(i) :
    ## 0. 전처리 전 N수 17449
    ## 1. ECG나 ABP 둘 중 하나라도 데이터가 없을 경우 제거
    ## 2. ABP의 std 값 20 미만인 것 제거
    ## 3. 총 수집시간 자체가 적은 경우 제거 (수술시간 대비 50% 미만 수집률)

    png_list = os.listdir('/srv/project_data/HPI_project/Preprocessing_1/')


    conn, cursor = jy.dbIn()
    test_conn, test_cursor = jy.test_dbIn()
    temp_split = i.split("_")
    try : ## vital file 자체가 깨져 있을 경우
        Vital_file = vr.VitalFile(vital_address+temp_split[0]+"/"+temp_split[1]+"/"+i)
    except :
        print(f"{i}_vital_error")
        error_list['vital_error'].append(i)
        return None
    sql = f"""
    select *
    from sa_api_waveinfofile as wave
    join sa_api_filerecorded as file
    on file.id = wave.record_id
    where file.file_basename like '{i}' and (wave.channel_name like '%ECG%' or wave.channel_name like '%ABP%' or wave.channel_name like '%IBP%');
    """
    cursor.execute(sql)
    waveinfo = pd.DataFrame(cursor.fetchall())
    try :
        ECG_time,ECG_data = Vital_file.get_samples(waveinfo['channel_name'][waveinfo.index[waveinfo['channel_name'].str.contains('ECG')].tolist()[0]])
        ABP_time,ABP_data = Vital_file.get_samples(waveinfo['channel_name'][waveinfo.index[waveinfo['channel_name'].str.contains('BP')].tolist()[0]])
    except :
        print(f"{i}_vital_error")
        error_list['vital_error'].append(i)
        return None

    if isinstance(ECG_data,type(None)) or isinstance(ABP_time,type(None)) : ## ECG나 ABP둘 중 하나라도 데이터가 없을 경우
        print(f"{i}_None_data")
        error_list['None_data'].append(i)
        return None

    if np.array(ABP_data).std() < 20 : ## ABP의 STD가 20미만이면 안함
        print(f"{i}_None_data")
        error_list['under_std'].append(i)
        fig = plt.figure(figsize=(24, 12))
        gs = GridSpec(5, 1, hspace=0.3, height_ratios=[1, 1, 0.05, 1, 1])

        ac = plt.subplot(gs[0])
        ac.set_title(f"{i}\nECG {np.array(ECG_data).std()}", fontsize=20)
        ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
        for axis in ['top', 'bottom', 'left', 'right']:
            ac.spines[axis].set_linewidth(2)
        ac.set_ylim(-1, 3)
        ac.plot(ECG_time[len(ECG_time) // 2:len(ECG_time) // 2 + 5000],
                ECG_data[len(ECG_time) // 2:len(ECG_time) // 2 + 5000])
        ac = plt.subplot(gs[1])
        ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
        for axis in ['top', 'bottom', 'left', 'right']:
            ac.spines[axis].set_linewidth(2)
        ac.set_ylim(-1, 3)
        ac.plot(ECG_time, ECG_data)
        ac = plt.subplot(gs[3])
        ac.set_title(f"ABP {np.array(ABP_data).std()}", fontsize=20)
        ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
        for axis in ['top', 'bottom', 'left', 'right']:
            ac.spines[axis].set_linewidth(2)
        ac.set_ylim(0,200)
        ac.plot(ABP_time[len(ABP_time) // 2:len(ABP_time) // 2 + 5000],
                ABP_data[len(ABP_time) // 2:len(ABP_time) // 2 + 5000])
        ac = plt.subplot(gs[4])
        ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
        for axis in ['top', 'bottom', 'left', 'right']:
            ac.spines[axis].set_linewidth(2)
        ac.set_ylim(0, 200)
        ac.plot(ABP_time, ABP_data)
        plt.savefig(f"/srv/project_data/HPI_project/Preprocessing_1/{i}.png")
        plt.close()
        return None

    # op_time = "20"+temp_split[1][:2]+"-"+temp_split[1][2:4]+"-"+temp_split[1][4:]
    # sql = f"""
    # select *
    # from operation as op
    # join location
    # on location.id = op.location_id
    # where location.name = '{temp_split[0]}' and start_date >= '{op_time} 00:00:00' and start_date <= '{op_time} 23:59:59'
    # """
    #
    # test_cursor.execute(sql)
    # op_info = test_cursor.fetchall()
    #
    # near_op = isNear(op_info,ABP_time[0]) ## 해당 Vital 파일이 어느 수술과 매칭되는지 체크
    # if near_op == 99 :
    #     print(f"{i}_under_op")
    #     error_list['under_op'].append(i)
    #     return None
    #
    # op_time_length = int(datetime.datetime.timestamp(op_info[near_op]['end_date']) - datetime.datetime.timestamp(op_info[near_op]['start_date']))
    # ABP_data_rate = (op_time_length * int(waveinfo['sampling_rate'][waveinfo.index[waveinfo['channel_name'].str.contains('BP')].tolist()[0]])) // 2
    # ECG_data_rate = (op_time_length * int(waveinfo['sampling_rate'][waveinfo.index[waveinfo['channel_name'].str.contains('ECG')].tolist()[0]])) // 2
    #
    # if len(ABP_time) < ABP_data_rate or len(ECG_data) < ECG_data_rate : ## ABP나 ECG 수집률이 수술기록 대비 50% 미만일 경우
    #     print(f"{i}_under_op")
    #     error_list['under_op'].append(i)
    #     return None

    # fig = plt.figure(figsize=(24, 12))
    # gs = GridSpec(5, 1, hspace=0.3, height_ratios=[1, 1, 0.05, 1, 1])
    #
    # ac = plt.subplot(gs[0])
    # ac.set_title(f"{i}\nECG {np.array(ECG_data).std()}", fontsize=20)
    # ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
    # for axis in ['top', 'bottom', 'left', 'right']:
    #     ac.spines[axis].set_linewidth(2)
    # ac.set_ylim(-1, 3)
    # ac.plot(ECG_time[len(ECG_time) // 2:len(ECG_time) // 2 + 5000],
    #         ECG_data[len(ECG_time) // 2:len(ECG_time) // 2 + 5000])
    # ac = plt.subplot(gs[1])
    # ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
    # for axis in ['top', 'bottom', 'left', 'right']:
    #     ac.spines[axis].set_linewidth(2)
    # ac.set_ylim(-1, 3)
    # ac.plot(ECG_time, ECG_data)
    # ac = plt.subplot(gs[3])
    # ac.set_title(f"ABP {np.array(ABP_data).std()}", fontsize=20)
    # ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
    # for axis in ['top', 'bottom', 'left', 'right']:
    #     ac.spines[axis].set_linewidth(2)
    # ac.set_ylim(0,200)
    # ac.plot(ABP_time[len(ABP_time) // 2:len(ABP_time) // 2 + 5000],
    #         ABP_data[len(ABP_time) // 2:len(ABP_time) // 2 + 5000])
    # ac = plt.subplot(gs[4])
    # ac.tick_params(axis='both', direction='in', labelsize=15, width=2)
    # for axis in ['top', 'bottom', 'left', 'right']:
    #     ac.spines[axis].set_linewidth(2)
    # ac.set_ylim(0, 200)
    # ac.plot(ABP_time, ABP_data)
    # plt.savefig(f"/srv/project_data/HPI_project/Preprocessing_1/{i}.png")
    # plt.close()
## end def

adrress = "/home/projects/pcg_transform/Meeting/jy/"
vital_address = "/srv/vital_amc/"
file_name = ['HPI_file_ver1.xlsx','HPI_file_ver1(not_ABP).xlsx']
file_list = os.listdir(adrress)

for file in file_list[:] :
    if file not in file_name :
        file_list.remove(file)

HPI_excel = pd.read_excel(adrress+file_list[1])


with Pool(8) as p :
    p.map(preprocessing_1,HPI_excel['file_name'])