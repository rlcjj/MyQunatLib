#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/2/3
"""

import os,sys,logging ,time,decimal,codecs,numpy 
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import pandas as pd


########################################################################
class BetaCalc(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,mktDataDbName,ifLoadMemoryDb,startDate):
        """Constructor"""
        mktDbPath = GetPath.GetLocalDatabasePath()["RawEquity"]+mktDataDbName
        if ifLoadMemoryDb==1:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            print "Load market data into memory database"
            cur.execute("ATTACH '{}' AS MktData".format(mktDbPath))
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,TC_Adj FROM MktData.A_Share_Data WHERE Date>='{}'".format(startDate))
            cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,TC FROM MktData.IndexData WHERE Date>='{}'".format(startDate))
            print "Done"
            print "Create index on table StockData and table IndexData"
            cur.execute("CREATE INDEX idS ON StockData (StkCode,Date)")
            cur.execute("CREATE INDEX idI ON IndexData (StkCode,Date)")
            print "Done"
        else:
            print "Directly connect to local database"
            self.conn = lite.connect(mktDbPath)
            self.conn.text_factory = str
            
            
    #----------------------------------------------------------------------
    def SetParameters(self,marketIndex,stockUniverse,returnHorizon,lookbackWindow):
        """"""
        self.marketIndex = marketIndex
        self.colName = [marketIndex]
        for stkName in stockUniverse:
            self.colName.append(stkName)
        self.mktIndex = marketIndex
        self.returnHorizon = returnHorizon
        self.lookbackWindow = lookbackWindow
        cur = self.conn.cursor()
        cur.execute("PRAGMA synchronous=OFF")
        sql = """
              CREATE TABLE DATA
              AS SELECT Date,TC FROM IndexData 
              WHERE IndexData.StkCode='{}'
              """
        cur.execute(sql.format(marketIndex))
        for stk in stockUniverse:
            sql1 = """
                   ALTER TABLE DATA ADD COLUMN '{}' FLOAT;
                   """
            cur.execute(sql1.format(stk))
            print stk
        for stk in stockUniverse: 
            sql2 = """
                   UPDATE DATA
                   SET '{}'=(SELECT TC_Adj
                             FROM StockData
                             WHERE DATA.Date=StockData.Date
                             AND StockData.StkCode='{}')
                   """
            cur.execute(sql2.format(stk,stk))
            print stk
        print "Start"
       

        
    #----------------------------------------------------------------------
    def Calc(self,date,stock):
        """"""
        tm1 = time.time()
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT *
                    FROM DATA
                    WHERE Date<='{}'
                    ORDER BY Date DESC LIMIT {}
                    """.format(date,self.lookbackWindow+1))
        vals = cur.fetchall()
        dt = numpy.array(vals)
        df = pd.DataFrame(dt[:,1:].astype(numpy.float),index=dt[:,0],columns=self.colName)
        dfRet = numpy.log(df/df.shift(-1))
        tm2 = time.time()
        print tm2-tm1
        betaDict = {}
        for stk in self.colName[1:]:
            dffRet = dfRet[[self.marketIndex,stk]]
            covMat = dffRet.cov().values
            _cov = covMat[0,1]
            _var = covMat[0,0]
            beta = covMat[0,1]/covMat[0,0]
            betaDict[stk] = beta
            print stk,beta
        print betaDict
            
        
        
        
        
            
  
import DefineInvestUniverse.GetIndexCompStocks as CompStks            
            
if __name__ == "__main__":    
    dbAddress = "MktGenInfo\\IndexComp_Wind_CICC.db"
    compStks = CompStks.GetIndexCompStocks(dbAddress)
    allStks = compStks.GetAllStocks('000300')
    betaCalc = BetaCalc("MktData\\MktData_Wind_CICC.db", 1,"20100127")
    betaCalc.SetParameters("000300",allStks,2,250)
    betaCalc.Calc("20160128","600837")
        
        
            
        
    