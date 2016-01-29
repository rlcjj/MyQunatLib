#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import numpy as np


########################################################################
class CalcPortRets(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,mktDataDbAddr,conn=None):
        """Constructor"""
        if conn!=None:
            self.conn = conn
        else:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            print "Load local database into in-memory database" 
            locDbPath = GetPath.GetLocalDatabasePath()
            _mktDataDbAddr = locDbPath["RawEquity"]+mktDataDbAddr
            cur.execute("ATTACH '{}' AS MktData".format(_mktDataDbAddr))
            cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,LC,TC FROM MktData.A_Share_Data")
            print "Finished"
            cur.execute("CREATE INDEX mId ON MktData (Date,StkCode)")
            print "Finished"    
            
    #----------------------------------------------------------------------
    def Calc(self,date,stkList):
        """"""
        if len(stkList)<=2:
            return 0
        else:
            cur = self.conn.cursor()
            ret = []
            for stk in stkList:
                sql = """
                      SELECT LC,TC
                      FROM MktData
                      WHERE StkCode='{}'
                      AND Date='{}'
                      """
                cur.execute(sql.format(stk,date))
                content = cur.fetchone()
                if content!=None and content[0]!=None and content[1]!=None:
                    ret.append(np.log(content[1])-np.log(content[0]))
                #print stk,date,np.log(content[1])-np.log(content[0])
            return np.mean(ret)
            
            