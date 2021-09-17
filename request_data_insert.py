import os
import numpy as np
import matplotlib.pyplot as plt


npz_list = os.listdir("/srv/data/MBP_data")

for i in npz_list :
    test_npz = np.load(f"/srv/data/MBP_data/{i}")
    # print(len(test_npz['data']))
    if len(test_npz['data']) == 0 :
        print(i)
    # plt.figure(figsize=(20,10))
    # plt.plot(test_npz['time'],test_npz['data'])
    # plt.show()
    # plt.close()

len(npz_list)