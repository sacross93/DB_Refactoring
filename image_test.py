import zAI
from zAI import zImage
import os
import tensorflow as tf
from PIL import Image
import cv2

address = "/home/projects/pcg_transform/Meeting/jy/"
file_list = os.listdir(address)


def get_zai_dir():
    '''
    Work out where to store configuration and model files locally
    '''
    _zAI_base_dir = os.path.expanduser(address)

    if not os.access(_zAI_base_dir, os.W_OK):
        _zAI_base_dir = '/temp'

    _zAI_dir = os.path.join(_zAI_base_dir, '.zAI')

    return _zAI_dir

print(get_zai_dir())

woohyun = zImage(address+"joke.png")
# img = Image.open(address+'joke.png')
# img.show()
# img_resize = img.resize((350,350),Image.LANCZOS)
# img_resize.save(address+'joke2.png')
# img_resize.show()


woohyun.find_faces()
woohyunPhto = woohyun.extract_face(margin=15)
woohyunPhto.display()
woohyun.display()

test_img = cv2.imread(address+"joke.png",cv2.IMREAD_COLOR)
cv2.namedWindow('test title')
cv2.imshow('test title',test_img)
cv2.waitKey(0)