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
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath


########################################################################
class GetFdmtFactorVal(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finDerivDataDbAddr,mktDataDbAddr,conn=None):
        """Constructor"""
        if conn!=None:
            self.conn = conn
        else:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            print "Load local database into in-memory database"        
            locDbPath = GetPath.GetLocalDatabasePath()
            _finDerivDataDbAddr = locDbPath["ProcEquity"]+finDerivDataDbAddr
            _mktDataDbAddr = locDbPath["RawEquity"]+mktDataDbAddr
            cur.execute("ATTACH '{}' AS FinRpt".format(_finDerivDataDbAddr))
            cur.execute("ATTACH '{}' AS MktData".format(_mktDataDbAddr))
            cur.execute("CREATE TABLE FinRptDerivData AS SELECT * FROM FinRpt.FinRptDerivData")
            cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,TC FROM MktData.Price_Volume")
            print "Finished"
            print "Create index"
            cur.execute("CREATE INDEX mId ON MktData (Date,StkCode)")
            cur.execute("CREATE INDEX fId ON FinRptDerivData (DeclareDate,StkCode)")
            print "Finished"        
        
    
    #----------------------------------------------------------------------
    def GetVal(self,lookupDate,effectiveDays,stkCode,algo):
        """"""
        tm1 = time.time()
        
        cur = self.conn.cursor()
        
        _lookupDate = datetime.strptime(lookupDate,"%Y%m%d")
        _lookupLimit = _lookupDate - timedelta(days=effectiveDays)
        lookupLimit = _lookupLimit.strftime("%Y%m%d")    
        date = (lookupLimit,lookupDate)
        indicator = algo.Calc(cur,date,stkCode)
        tm2 = time.time()
        #print "Time consume:{}".format(tm2-tm1)
        return indicator    