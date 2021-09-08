from influxdb import InfluxDBClient
import pymysql
import jyLibrary as jy
import datetime
import copy
from multiprocessing import *

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

def influxInsert(rIDs) :
    for rid in rIDs:
        mysql_query = """select * from number_gec where record_id = %s; """""
        a = cursor.execute(mysql_query, (rid['id']))
        gecs = cursor.fetchall()
        gecsList = []
        for gec in gecs:
            id = gec['id']
            dt = gec['dt']
            del gec['id']
            del gec['dt']
            del gec['operation_id']
            del gec['record_id']

            tempGec = copy.deepcopy(gec)
            for key in tempGec.keys():
                # for key in keys :
                if gec[key] == None:
                    del gec[key]
                    # print(key)
                # gec.pop(key,None)

                test = {
                    "measurement": "test_number_gec3",
                    "tags": {"id": id},
                    "time": dt,
                    "fields": gec
                }
                gecsList.append(test)
                # print(test)
        print("insert...")
        temp = 0
        while True:
            if len(gecsList) <= 10000:
                influxCli.write_points(gecsList)
            elif len(gecsList) > temp + 10000:
                influxCli.write_points(gecsList[temp:temp + 10000])
            else:
                influxCli.write_points(gecsList[temp:-1])
            temp += 10000

        print("finish")

# result = influxCli.query("""select value from test_AUDIO""")



mysql_query = """select distinct * from sa_api_filerecorded """""
a = cursor.execute(mysql_query)
rids = cursor.fetchall()


# with Pool(6) as p :
#     parResult = p.map(influxInsert,rids)


# mysql_query = """select * from number_gec where record_id = %s; """""
# a = cursor.execute(mysql_query, (rids[0]['id']))
# b = cursor.fetchall()


keys=['ABP_SBP', 'ABP_DBP', 'ABP_MBP', 'ABP_HR', 'AGENT_ET', 'AGENT_FI', 'AGENT_IN', 'AGENT_MAC',
      'ARRH_ECG_HR', 'BAL_GAS_ET', 'BIS_BIS', 'BIS_BSR', 'BIS_EMG', 'BIS_SQI', 'BT_AXIL',
      'BT_PA', 'BT_ROOM', 'CO', 'COMPLIANCE', 'CO2_AMB_PRESS', 'CO2_ET', 'CO2_ET_PERCENT',
      'CO2_FI', 'CO2_RR', 'CO2_IN', 'CO2_IN_PERCENT', 'CVP', 'ECG_HR', 'ECG_HR_ECG', 'ECG_HR_MAX',
      'ECG_HR_MIN', 'ECG_IMP_RR', 'ECG_ST', 'ECG_ST_AVF', 'ECG_ST_AVL', 'ECG_ST_AVR', 'ECG_ST_I',
      'ECG_ST_II', 'ECG_ST_III', 'ECG_ST_V', 'EEG_FEMG', 'ENT_BSR', 'ENT_EEG', 'ENT_EMG',
      'ENT_RD_BSR', 'ENT_RD_EEG', 'ENT_RD_EMG', 'ENT_RE', 'ENT_SE', 'ENT_SR', 'EPEEP',
      'FEM_SBP', 'FEM_DBP', 'FEM_MBP', 'FEM_HR', 'HR', 'ICP', 'IE_RATIO', 'LAP', 'MAC_AGE',
      'MV', 'N2O_ET', 'N2O_FI', 'N2O_IN', 'NIBP_DBP', 'NIBP_SBP', 'NIBP_HR', 'NIBP_MBP',
      'NMT_CURRENT', 'NMT_PTC_CNT', 'NMT_PULSE_WIDTH', 'NMT_T1', 'NMT_T4_T1', 'NMT_TOF_CNT',
      'O2_ET', 'O2_FE', 'O2_FI', 'PA_SBP', 'PA_DBP', 'PA_MBP', 'PA_HR', 'PCWP', 'PEEP', 'PLETH_HR',
      'PLETH_IRAMP', 'PLETH_SPO2', 'PPEAK', 'PPLAT', 'PPV', 'RAP', 'RR', 'RR_VENT', 'RVEF', 'RVP',
      'SPI', 'SPV', 'TOF_T1', 'TOF_T2', 'TOF_T3', 'TOF_T4', 'TV_EXP', 'TV_INSP']


mysql_query = """desc number_gec;"""""
a = cursor.execute(mysql_query)
gecs = cursor.fetchall()

rids[0]['bed_id']




cancelQuery=datetime.datetime.fromtimestamp(1560209164.849000000)

rids[0]
for rid in rids:
    mysql_query="""select * from number_gec where record_id = %s and %s > %s; """""
    a= cursor.execute(mysql_query, (rid['id'],rid['end_date'],cancelQuery))
    gecs=cursor.fetchall()
    gecsList=[]
    bed_id = rid['bed_id']
    print(rid['begin_date'],"start")
    for gec in gecs :
        if gec['dt'] < cancelQuery :
            print(gec['dt'])
            break
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
                "measurement" : "test_number_gec4",
                "tags" : { "bed_id" : bed_id },
                "time" : dt,
                "fields" : gec
            }
            gecsList.append(test)
            # print(test)
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


    print("finish")
    # print(i)

#
# td = {'id': 21887724,
#       'dt': datetime.datetime(2019, 6, 10, 4, 36, 42, 803000),
#       'ABP_SBP': None, 'ABP_DBP': None, 'ABP_MBP': None, 'ABP_HR': None,
#       'AGENT_ET': 0.0, 'AGENT_FI': 0.0, 'AGENT_IN': None, 'AGENT_MAC': 0.01,
#       'ARRH_ECG_HR': None, 'BAL_GAS_ET': 77.45, 'BIS_BIS': None, 'BIS_BSR': None, 'BIS_EMG': None,
#       'BIS_SQI': None, 'BT_AXIL': None, 'BT_PA': None, 'BT_ROOM': None, 'CO': None,
#       'COMPLIANCE': None, 'CO2_AMB_PRESS': 758.1, 'CO2_ET': 0.02, 'CO2_ET_PERCENT': None,
#       'CO2_FI': 0.02, 'CO2_RR': None, 'CO2_IN': None, 'CO2_IN_PERCENT': None, 'CVP': None,
#       'ECG_HR': 58.9, 'ECG_HR_ECG': None, 'ECG_HR_MAX': None, 'ECG_HR_MIN': 0.0, 'ECG_IMP_RR': None,
#       'ECG_ST': None, 'ECG_ST_AVF': None, 'ECG_ST_AVL': None, 'ECG_ST_AVR': None, 'ECG_ST_I': None,
#       'ECG_ST_II': None, 'ECG_ST_III': None, 'ECG_ST_V': None, 'EEG_FEMG': None, 'ENT_BSR': None,
#       'ENT_EEG': None, 'ENT_EMG': None, 'ENT_RD_BSR': None, 'ENT_RD_EEG': None, 'ENT_RD_EMG': None,
#       'ENT_RE': None, 'ENT_SE': None, 'ENT_SR': None, 'EPEEP': None, 'FEM_SBP': None, 'FEM_DBP': None,
#       'FEM_MBP': None, 'FEM_HR': None, 'HR': None, 'ICP': None, 'IE_RATIO': None, 'LAP': None,
#       'MAC_AGE': 0.01, 'MV': None, 'N2O_ET': 0.51, 'N2O_FI': 0.51, 'N2O_IN': None, 'NIBP_DBP': None,
#       'NIBP_SBP': None, 'NIBP_HR': None, 'NIBP_MBP': None, 'NMT_CURRENT': None, 'NMT_PTC_CNT': None,
#       'NMT_PULSE_WIDTH': None, 'NMT_T1': None, 'NMT_T4_T1': None, 'NMT_TOF_CNT': None, 'O2_ET': 22.02,
#       'O2_FE': None, 'O2_FI': 22.02, 'PA_SBP': None, 'PA_DBP': None, 'PA_MBP': None, 'PA_HR': None,
#       'PCWP': None, 'PEEP': None, 'PLETH_HR': 58.9, 'PLETH_IRAMP': 4.7, 'PLETH_SPO2': 96.0,
#       'PPEAK': None, 'PPLAT': None, 'PPV': None, 'RAP': None, 'RR': None, 'RR_VENT': None,
#       'RVEF': None, 'RVP': None, 'SPI': 0.0, 'SPV': None, 'TOF_T1': None, 'TOF_T2': None,
#       'TOF_T3': None, 'TOF_T4': None, 'TV_EXP': None, 'TV_INSP': None, 'record_id': 1,
#       'operation_id': None}
#
# td = [
#     {
#         "measurement": "test_number_gec1",
#         "tags": { "id": b[0]['id'] },
#         "time": datetime.datetime(2019,6,10,4,36,42,803000),
#         "fields": {
#         "ABP_SBP": b[0]['ABP_SBP'],
#         "ABP_MBP": b[0]['ABP_MBP']
#         }
#     }
# ]
#
#
#
# td_list=[]
#
# td_list.append(gecsList[0])
#
# influxCli.write_points(td_list)
#
# #
# # b[0]['dt']
#
# gecsList[0]

# count=0
# for g in range(len(gecsList)) :
#     if gecsList[g] :
#         count += 1
#     if count == 99 :
#         print(g)
#         break
#     print(g)
#
#
#
# td = [
#     {
#         "measurement": "test_number_gec2",
#         "tags": { "id": 1},
#         "time": 1560141402803990000,
#         "fields": { "AGENT_ET" : 0.0
#         }
#     }
# ]
# influxCli.write_points(td)
