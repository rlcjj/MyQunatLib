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
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,TC_Adj,Statu FROM MktData.A_Share_Data WHERE Date>='{}'".format(startDate))
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
        self.lookbackWindow = lookbackWindow
        self.marketIndex = marketIndex
        cur = self.conn.cursor()
        
        cur.execute("SELECT Date,TC FROM IndexData WHERE StkCode='{}'".format(marketIndex))
        vals=cur.fetchall()
        dt = numpy.array(vals)
        dfi = pd.DataFrame(dt[:,1].astype(numpy.float),index=dt[:,0],columns=[marketIndex])
        dfList = [dfi]
        for stk in stockUniverse:
            
            cur.execute("SELECT Date,(CASE WHEN Statu=-1 THEN TC_Adj ELSE Null END) FROM StockData WHERE StkCode='{}'".format(stk))
            vals=cur.fetchall()
            if len(vals)>0:
                dt = numpy.array(vals)
                #print stk
                dfs = pd.DataFrame(dt[:,1].astype(numpy.float),index=dt[:,0],columns=[stk])     
                dfList.append(dfs)
            
        self.df = pd.concat(dfList,axis=1)
        self.trdDay = self.df.index
        self.df = self.df[::returnHorizon]
        self.retDf = numpy.log(self.df/self.df.shift(1))

        
    #----------------------------------------------------------------------
    def Calc(self,date):
        """"""
        pos = self.trdDay.tolist().index(date)
        print pos
        startDate = self.trdDay[pos-self.lookbackWindow]
        print startDate
        dfRet = self.retDf[(self.retDf.index>=startDate)*(self.retDf.index<=date)]
        effectiveNum = int(len(dfRet.index)*0.6)
        print dfRet
        betaDict = {}
        for stk in dfRet.columns:
            dffRet = dfRet[[self.marketIndex,stk]]
            covMat = dffRet.cov(effectiveNum).values
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
    betaCalc.SetParameters("000300",allStks,5,250)
    betaCalc.Calc("20160128")
        
        
            
        
    