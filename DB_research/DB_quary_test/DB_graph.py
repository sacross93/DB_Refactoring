import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime
import jyLibrary as jy

a=pd.read_excel('/mnt/jy_excel/DB_list.xlsx',engine='openpyxl')

c=a['end time']-a['start time']

type(a['end time'][0])
type(a['end time'][0].to_pydatetime())

a['start time'] = pd.to_datetime(a['start time'], format='%Y-%m-%d %H:%M:%S', errors='raise')
a['end time'] = pd.to_datetime(a['end time'], format='%Y-%m-%d %H:%M:%S', errors='raise')

temparray=[]
for i in range(len(a['end time'])) :
    temparray.append(i+1)

plt.figure(figsize=(20, 10))
b=plt.plot(temparray,a['time'])
plt.show()

