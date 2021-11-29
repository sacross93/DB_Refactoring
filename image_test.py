import os
from PIL import Image
import matplotlib.pylab as plt
import cv2
import random

address = "/home/projects/pcg_transform/Meeting/jy/"
file_list = os.listdir(address)

for k in range(1000) :
    test_img = Image.open(address+"joke.png")
    test_img = test_img.resize((600,600))

    rgb_result = []
    for i in range(0,test_img.size[0]):
        temp = (random.randrange(0,256),random.randrange(0,256),random.randrange(0,256))
        for j in range(0,test_img.size[1]):
            rgb = test_img.getpixel((i,j))
            rgb_a = temp
            test_img.putpixel((i, j), rgb_a)

    for i in range(test_img.size[0]) :
        select_vr = random.randrange(0,2)
        select_re = random.randrange(0,2)
        temp = (random.randrange(0, 256), random.randrange(0, 256), random.randrange(0, 256))
        for j in range(test_img.size[1]) :
            rgb_a = temp
            if select_vr == 0 :
                if select_re == 0 :
                    test_img.putpixel((i,j),rgb_a)
                else :
                    test_img.putpixel((test_img.size[0]-i-1, j), rgb_a)
            else :
                if select_re == 0 :
                    test_img.putpixel((j,i),rgb_a)
                else :
                    test_img.putpixel((test_img.size[1]-j-1, i), rgb_a)

    # plt.imshow(test_img)
    # plt.show()
    test_img.save(f"/home/projects/pcg_transform/Meeting/jy/img_test/test_img_{k}.png")

test_img.size[1]-j+1