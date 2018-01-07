# -*- coding: utf8 -*-
'''
Created on 2017-11-14

@author: miscy210 <miscy210@163.com>
'''
import os
import platform

def get_packagelist():
    result = os.popen("pip list")
    if platform.system() == "Windows":
        plst = result.split('\r\n')
    elif platform.system() == "Linux":
        plst = result.split('\n')
    else:
        plst = result.split('\r')
    packageslist = [item.split(' ')[0] for item in plst]
    return packageslist

def pipupgrade(pl):
    for p in pl:
        os.system("pip install --upgrade %s"%p)

if __name__ == "__main__":
    pipupgrade(get_packagelist())
