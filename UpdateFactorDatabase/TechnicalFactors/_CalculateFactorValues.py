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

import Tools.GetLocalDatabasePath as GetPath
import Tools.LogOutputHandler as LogHandler
import Configs.RootPath as Root
RootPath = Root.RootPath


########################################################################
class CalculateFactorValues(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,dbPathMarketData,conn=None,logger=None):
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
            _dbPathMarketData = locDbPath["EquityDataRaw"]+dbPathMarketData
            cur.execute("ATTACH '{}' AS MktData".format(_dbPathMarketData))
            
            self.logger.info("<{}>-Load table MarketData".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,TC,LC,TC_Adj,Vol,Amt,Statu FROM MktData.AStockData WHERE Date>='20060101'")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Load talbe MarketCap".format(__name__.split('.')[-1]))
            cur.execute("CREATE TABLE MktCap AS SELECT * FROM MktData.MarketCap")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1])) 
            
            self.logger.info("<{}>-Create index on table MarketData".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX mId ON MktData (StkCode,Date)")
            self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))
            self.logger.info("<{}>-Create index on table MarketCap".format(__name__.split('.')[-1]))
            cur.execute("CREATE INDEX cId ON MktCap (StkCode,Date)")
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
              SELECT MktData.Date,TC,TC_Adj,Vol,Amt,Statu,FloatCap,TotCap 
              FROM MktData 
              LEFT JOIN MktCap
              ON MktData.Date=MktCap.Date AND MktData.StkCode=MktCap.StkCode
              WHERE MktData.StkCode='{}' AND MktData.Date>='{}'
              ORDER BY MktData.Date ASC
              """
        cur.execute(sql.format(stkCode,begDate))
        rows = cur.fetchall()
        mat = np.array(rows)
        df = pd.DataFrame(mat[:,1:],index=mat[:,0],columns=["price","price_adj","vol","amt","statu","f_cap","t_cap"])
        df = df.fillna(method='ffill')
        df.to_csv("df.csv")
        print df
        

        factorVal = algo.Calc(df)
        print factorVal

        
        