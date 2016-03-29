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


########################################################################
class CalcFactorVals(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finDerivDataDbAddr,mktDataDbAddr,conn=None,logger=None):
        """Constructor"""
        
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("CalcFactorVals.log")
        else:    
            self.logger = logger        
        
        #Load data into in-memory database
        if conn!=None:
            self.conn = conn
        else:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            
            self.logger.info("Load local database into in-memory database...")        
            locDbPath = GetPath.GetLocalDatabasePath()
            _finDerivDataDbAddr = locDbPath["ProcEquity"]+finDerivDataDbAddr
            _mktDataDbAddr = locDbPath["RawEquity"]+mktDataDbAddr
            cur.execute("ATTACH '{}' AS FinRpt".format(_finDerivDataDbAddr))
            cur.execute("ATTACH '{}' AS MktData".format(_mktDataDbAddr))
            
            self.logger.info("Load table FinRptDerivData")
            cur.execute("CREATE TABLE FinRptDerivData AS SELECT * FROM FinRpt.FinRptDerivData")
            self.logger.info("Done")
            self.logger.info("Load table ForecastData")
            cur.execute("CREATE TABLE FcstData AS SELECT * FROM FinRpt.ForecastData")
            self.logger.info("Done")
            self.logger.info("Load table MarketData")
            cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,TC,LC,TC_Adj FROM MktData.A_Share_Data WHERE Date>='20060101'")
            self.logger.info("Done")
            self.logger.info("Load talbe MarketCap")
            cur.execute("CREATE TABLE MktCap AS SELECT * FROM MktData.MarketCap")
            self.logger.info("Done") 
            
            self.logger.info("Create index on table FinRptDerivData")
            cur.execute("CREATE INDEX fiId ON FinRptDerivData (StkCode,DeclareDate)")
            self.logger.info("Done")
            self.logger.info("Create index on table ForecastData")
            cur.execute("CREATE INDEX fcId ON FcstData (StkCode,DeclareDate)")
            self.logger.info("Done")
            self.logger.info("Create index on table MarketData")
            cur.execute("CREATE INDEX mId ON MktData (StkCode,Date)")
            self.logger.info("Done")
            self.logger.info("Crate index on table MarketCap")
            cur.execute("CREATE INDEX cId ON MktCap (StkCode,Date)")
            self.logger.info("Done")            
    
    
    #----------------------------------------------------------------------
    def Calc(self,lookupDate,effectiveDays,stkCode,algos):
        """"""
        tm1 = time.time()
        
        cur = self.conn.cursor()
        
        _lookupDate = datetime.strptime(lookupDate,"%Y%m%d")
        _lookupLimit = _lookupDate - timedelta(days=effectiveDays)
        lookupLimit = _lookupLimit.strftime("%Y%m%d")    
        date = (lookupLimit,lookupDate)
        
        begDate = date[0] 
        endDate = date[1]        

        sql = """
              SELECT TC
              FROM MktData
              WHERE StkCode='{}'
                    AND Date>='{}'
                    AND Date<='{}'
              ORDER BY Date DESC LIMIT 1
              """
        cur.execute(sql.format(stkCode,begDate,endDate))
        content = cur.fetchone()
        if content==None:
            return None
        p = content[0]    
    
        sql = """
              SELECT TotCap
              FROM MktCap
              WHERE StkCode='{}'
                    AND Date<='{}'
              ORDER BY Date DESC LIMIT 1
              """
        cur.execute(sql.format(stkCode,endDate))
        content = cur.fetchone()
        if content==None:
            return None  
        s = content[0]     
        
        sql = """
              SELECT DISTINCT AcctPeriod,ReportType
              FROM FinRptDerivData
              WHERE StkCode='{}'
                    AND DeclareDate<='{}'
              ORDER BY AcctPeriod DESC LIMIT 1
              """
        cur.execute(sql.format(stkCode,endDate))
        content = cur.fetchone()
        if content==None:
            return None
        acctPeriods = content[0]
        rptType = content[1]
        
        factorVals = []
        for algo in algos:
            factorVal = algo.Calc(cur,acctPeriods,p,s,date,stkCode)
            factorVals.append(factorVal)
        tm2 = time.time()
        #print "Time consume:{}".format(tm2-tm1)
        return acctPeriods,rptType,factorVals  