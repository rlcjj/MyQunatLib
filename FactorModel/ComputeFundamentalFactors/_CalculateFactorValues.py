#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
from datetime import datetime,timedelta

root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import Tools.LogOutputHandler as LogHandler


########################################################################
class CalculateFactorValues(object):
    """
    计算基本面因子的值
    """

    #----------------------------------------------------------------------
    def __init__(self,dbPathPITFundamentalData,dbPathMarketData,conn=None,logger=None):
        """Constructor"""
        
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("CalculateFundamentalFactorValues.log")
        else:    
            self.logger = logger        
        
        #Load data into in-memory database
        if conn!=None:
            self.conn = conn
        else:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            
            self.logger.info("<{}>-Load local database into in-memory database...".format(__name__.split('.')[-1]))        
            locDbPath = GetPath.GetLocalDatabasePath()
            _dbPathPITFundamentalData = locDbPath["ProcEquity"]+dbPathPITFundamentalData
            _dbPathMarketData = locDbPath["RawEquity"]+dbPathMarketData
            cur.execute("ATTACH '{}' AS FdmtData".format(_dbPathPITFundamentalData))
            cur.execute("ATTACH '{}' AS MktData".format(_dbPathMarketData))
            
            self.logger.info("<{}>-Load table FinancialPointInTimeData".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE FinancialPITData AS SELECT * FROM FdmtData.FinancialPointInTimeData")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Load table ForecastPintInTimeData".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE ForecastPITData AS SELECT * FROM FdmtData.ForecastPointInTimeData")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Load table MarketData".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,TC,LC,TC_Adj FROM MktData.AStockData WHERE Date>='20060101'")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Load talbe MarketCap".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE MktCap AS SELECT * FROM MktData.MarketCap")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1])) 
            
            self.logger.info("<{}>-Create index on table FinancialPITData".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX fiId ON FinancialPITData (StkCode,DeclareDate)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Create index on table ForecastData".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX fcId ON ForecastPITData (StkCode,DeclareDate)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Create index on table MarketData".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX mId ON MktData (StkCode,Date)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Create index on table MarketCap".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX cId ON MktCap (StkCode,Date)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))            
    
    
    #----------------------------------------------------------------------
    def Calculate(self,factorValDate,rptEffectiveDays,stkCode,factorAlgos):
        """
        计算并储存
        """
        cur = self.conn.cursor()
        
        _lookupDate = datetime.strptime(factorValDate,"%Y%m%d")
        _lookupLimit = _lookupDate - timedelta(days=rptEffectiveDays)
        lookupLimit = _lookupLimit.strftime("%Y%m%d")    
        date = (lookupLimit,factorValDate)
        
        begDate = date[0] 
        endDate = date[1]    
        #tm1 = time.time()
        #self.logger.info("<{}>-Calculating factor value of stock {} at {}".format(__name__.split('.')[-1],stkCode,factorValDate))
        #tm1 = time.time()
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
        #tm2 = time.time()
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
        #tm3 = time.time()
        sql = """
              SELECT DISTINCT AcctPeriod,ReportType
              FROM FinancialPITData
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
        #tm4 = time.time()
        #self.logger.info("<{}>-Basic info [Price:{},Capital:{},FinacialYear:{},ReportType:{}]".format(__name__.split('.')[-1],p,s,acctPeriods,rptType))    
        #print tm3-tm2,tm3-tm2,tm2-tm1
        factorVals = []
        for algo in factorAlgos:
            #tm1 = time.time()
            factorVal = algo.Calc(cur,acctPeriods,p,s,date,stkCode)
            #tm2 =time.time()
            #print tm2-tm1
            factorVals.append(factorVal)
        return acctPeriods,rptType,factorVals  