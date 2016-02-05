#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/2/5
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath

#----------------------------------------------------------------------
def Combine(mktDatabaseName,startYear,*indexCode):
    """"""
    mktDbPath = GetPath.GetLocalDatabasePath()["RawEquity"]+mktDataDbName
    conn = lite.connect(mktDbPath)    
    
    