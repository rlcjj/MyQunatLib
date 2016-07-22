#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/4
"""

import sqlite3 as lite
from datetime import datetime,timedelta
import numpy as np
import pandas as pd
import time
import multiprocessing

import Tools.GetLocalDatabasePath as GetPath
import Tools.LogOutputHandler as LogHandler
import Configs.RootPath as Root
RootPath = Root.RootPath


########################################################################
class CalculateFactorValues(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,dbPathMarketData,dbPathConstituentData,conn=None,logger=None):
        """Constructor"""
        
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("CalculateTechnicalFactorValues.log")
        else:    
            self.logger = logger        
        
        #Load data into in-memory database
        if conn!=None:
            self.conn = conn
        else:
            self.conn = lite.connect(":memory:",check_same_thread=False)
            self.conn.text_factory = str
            cur = self.conn.cursor()
            
            self.logger.info("<{}>-Load local database into in-memory database...".format(__name__.split('.')[-1]))        
            locDbPath = GetPath.GetLocalDatabasePath()
            _dbPathMarketData = locDbPath["EquityDataRaw"]+dbPathMarketData
            cur.execute("ATTACH '{}' AS MktData".format(_dbPathMarketData))
            _dbPathConstituentData = locDbPath["EquityDataRaw"]+dbPathConstituentData
            cur.execute("ATTACH '{}' AS ConstituentData".format(_dbPathConstituentData))            
            
            
            self.logger.info("<{}>-Load table MarketData".format(__name__.split('.')[-1]))
            cur.execute("""CREATE TABLE MktData AS SELECT StkCode,Date,TC,LC,TC_Adj,Vol,Amt,Statu,
                           SmallOrderDiff,MiddleOrderDiff,BigOrderDiff,InstOrderDiff,InBOrderDiff,
                           SmallOrderDiffActive,MiddleOrderDiffActive,BigOrderDiffActive,InstOrderDiffActive,InBOrderDiffActive,
                           NetInFlow,NetInFlowRatio FROM MktData.AStockData WHERE Date>='20060101'""")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Load talbe MarketCap".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE MktCap AS SELECT * FROM MktData.MarketCap")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Load talbe IndexData".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE IndexData AS SELECT Date,StkCode,TC FROM MktData.IndexData")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))             
            self.logger.info("<{}>-Load talbe ConstituentData".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE Constituent AS SELECT StkCode,StkName,IncDate,IndusCode,IndusName FROM ConstituentData.SWIndustry1st")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))             
            
            self.logger.info("<{}>-Create index on table MarketData".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX mId ON MktData (StkCode,Date)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Create index on table IndexData".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX iId ON IndexData (StkCode,Date)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))            
            self.logger.info("<{}>-Create index on table MarketCap".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX cId ON MktCap (StkCode,Date)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))       
            self.logger.info("<{}>-Create index on table Constituent".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX csId ON Constituent (StkCode,IncDate)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))               


    #----------------------------------------------------------------------
    def GetAllStockCodes(self):
        """
        Get all stocks in Fundamental PIT database 
        """
        self.logger.info("<{}>-Get all stock code in market database".format(__name__.split('.')[-1]))
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT StkCode FROM MarketCap")
        rows = cur.fetchall()
        allStks = []
        for row in rows:
            allStks.append(row[0])
        return allStks
    
    
    #----------------------------------------------------------------------
    def Calculate(self,begDate,stkCode,factorAlgos):
        """
        计算并储存
        """
        cur = self.conn.cursor() 
        sql = """
              SELECT T1.Date,T1.TC,T1.TC_Adj,T1.Vol,T1.Amt,T1.Statu,FloatCap,TotCap,I1.TC AS I000300,I2.TC AS I000905,
              SmallOrderDiff,MiddleOrderDiff,BigOrderDiff,InstOrderDiff,
              SmallOrderDiffActive,MiddleOrderDiffActive,BigOrderDiffActive,InstOrderDiffActive,
              NetInFlow,NetInFlowRatio,StkName,IncDate,IndusCode,IndusName
              FROM 
                  (SELECT MktData.*,MktCap.*
                   FROM MktData 
                   LEFT JOIN MktCap
                   ON MktData.Date>=MktCap.Date AND MktData.StkCode=MktCap.StkCode
                   WHERE MktData.StkCode='{}' AND MktData.Date>='{}'
                   GROUP BY MktData.Date 
                   ORDER BY MktData.Date ASC) T1
              LEFT JOIN Constituent T2
              ON T1.Date>=T2.IncDate AND T1.StkCode=T2.StkCode
              LEFT JOIN IndexData I1 ON I1.date=T1.Date and I1.StkCode='000300'
              LEFT JOIN IndexData I2 ON I2.date=T1.Date and I2.StkCode='000905'
              GROUP BY T1.Date
              """
        cur.execute(sql.format(stkCode,begDate))
        rows = cur.fetchall()
        if len(rows)==0:
            return None
        mat = np.array(rows)
        mat1 = mat[:,0]
        mat2 = mat[:,1:20].astype(float)
        mat3 = mat[:,20:]
        df = pd.DataFrame(mat2,index=mat1,columns=["price","price_adj","vol","amt","statu","f_cap","t_cap",
                                                   "I000300","I000905",
                                                   "SmallOrderDiff","MiddleOrderDiff","BigOrderDiff","InstOrderDiff",
                                                   "SmallOrderDiffActive","MiddleOrderDiffActive","BigOrderDiffActive","InstOrderDiffActive",
                                                   "NetInFlow","NetInFlowRatio"])
        info = pd.DataFrame(mat3,index=mat1,columns=["StkName","IncDate","IndusCode","IndusName"])
        #df = df.fillna(method='ffill')
        pool = multiprocessing.Pool(processes=8)
        result = []
        _results = []
        for i in xrange(len(factorAlgos)):
            result.append(pool.apply_async(factorAlgos[i].Calc, args=(df,)))
        pool.close()
        pool.join()
        for res in result:     
            _results.append(res.get())
        results = pd.concat(_results,axis=1)
        return results,info

        
        