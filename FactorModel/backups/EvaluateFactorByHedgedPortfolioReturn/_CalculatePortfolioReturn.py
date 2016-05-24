#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,logging,time,decimal,codecs
import numpy as np
import sqlite3 as lite
import time
from datetime import datetime,timedelta

import Tools.GetLocalDatabasePath as GetPath



########################################################################
class CalculatePortfolioReturn(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,mktDataDbAddr,conn=None,logger=None):
        """Constructor"""
        if logger == None:
            self.logger = logging.Logger("")
        else:
            self.logger = logger
            
        if conn!=None:
            self.conn = conn
        else:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            self.logger.info("<{}>-Load local database into in-memory database".format(__name__.split('.')[-1])) 
            locDbPath = GetPath.GetLocalDatabasePath()
            _mktDataDbAddr = locDbPath["RawEquity"]+mktDataDbAddr
            cur.execute("ATTACH '{}' AS MktData".format(_mktDataDbAddr))
            cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,LC,TC FROM MktData.AStockData")
            cur.execute("CREATE INDEX mId ON MktData (Date,StkCode)".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Done")
            
            
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
                      WHERE Date='{}'
                      AND StkCode='{}'
                      """
                cur.execute(sql.format(date,stk))
                content = cur.fetchone()
                if content!=None and content[0]!=None and content[1]!=None:
                    ret.append((content[1]-content[0])/content[0])
                #print stk,date,np.log(content[1])-np.log(content[0])
            return np.mean(ret)
            
            