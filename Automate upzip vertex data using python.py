# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 12:27:03 2019
@author: Danielove
"""

import re
import os
from os.path import isfile, join
import zipfile
import shutil

# 1. Unzip files to location
original3 = 'F:\\Raw Data\\'
target = 'D:\\CL\\'

df3 = []
for a in os.listdir(original3):
    if 'CL2' in a:
        for b in os.listdir(join(original3, a + "\\")):
            print(b)
            for c in os.listdir(join(original3, a + "\\" + b + "\\")):
                try:
                    First =  a + '\\' + b + "\\" + c
                    print(First)
                    df3.append(First)
                except:
                    continue

df3 = [i for i in df3 if '2017' in i or '201801' in i or '201802' in i or '201803' in i or '201804' in i]
list3 = []
#df2 = [i for i in df2 if 'ZC' in i]
#df2 = list2.copy()
i = 0
while i < len(df3):
    try:
        unzip = zipfile.ZipFile(original3 + df3[i], 'r')
        unzip.extractall(path = target + "\\".join(df3[i].split("\\")[:2]) + "\\")
        unzip.close()
        print(str(i) + "/" + str(len(df3)) + " " + df3[i].split("\\")[-1])
    except:
        list3.append(df3[i])
        pass
    i += 1