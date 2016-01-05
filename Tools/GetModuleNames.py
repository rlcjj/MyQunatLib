#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/5
"""

import os

#----------------------------------------------------------------------
def GetModuleNames(path):
    """"""
    files = os.listdir(path)
    modNames = []
    for f in files:
        modName = f.split('.')
        if modName[0][0]!='_' and modName[1]=='py':
            modNames.append(modName[0])
    return modNames