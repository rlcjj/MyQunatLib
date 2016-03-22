#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/3/10
"""

import os,sys,logging ,time,decimal,codecs,numpy 
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import pandas as pd
import DefineInvestUniverse.GetIndexCompStocks as InvestUniver   
from pandas.stats.api import ols
from scipy import linalg

########################################################################
class SignalStudy(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,mktDataDbName,rawDataStartDate,stockData=0,fundData=0):
        """Constructor"""
        mktDbPath = GetPath.GetLocalDatabasePath()["RawEquity"]+mktDataDbName
        self.conn = lite.connect(":memory:")
        self.conn.text_factory = str
        cur = self.conn.cursor()
        print "Load market data into memory database"
        cur.execute("ATTACH '{}' AS MktData".format(mktDbPath))
        self.appendStockData = stockData
        if stockData==1:
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,OP_Adj,HP_Adj,LP_Adj,TC_Adj,Statu,Vol,Amt FROM MktData.A_Share_Data WHERE Date>='{}'".format(rawDataStartDate))            
        self.appendFundData = fundData            
        if fundData==1:
            cur.execute("CREATE TABLE FundData AS SELECT StkCode,Date,OP_Adj,HP_Adj,LP_Adj,TC_Adj,Statu,Vol,Amt FROM MktData.ExchangeTradedFund WHERE Date>='{}'".format(rawDataStartDate))
        cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,OP,HP,LP,TC,Vol,Amt FROM MktData.IndexData WHERE Date>='{}'".format(rawDataStartDate))        
        print "Done"
        print "Create index on table StockData and table IndexData"
        if stockData==1:
            cur.execute("CREATE INDEX idS ON StockData (StkCode,Date)")
        if fundData==1:
            cur.execute("CREATE INDEX idF ON FundData (StkCode,Date)")
        cur.execute("CREATE INDEX idI ON IndexData (StkCode,Date)")
        print "Done"
            
        self.indexConstituents = InvestUniver.GetIndexCompStocks("MktGenInfo\\IndexComp_Wind_CICC.db")        
        self.rawDataStartDate = rawDataStartDate
        
    #----------------------------------------------------------------------
    def LoadDataIntoTimeSeries(self,benchMarkIndex,stkUniverIndex,etfs):
        """"""
        self.benchMarkIndex = benchMarkIndex
        self.stkUniverIndex = stkUniverIndex
        self.etfs = etfs
        
        data = {}
        
        cur = self.conn.cursor()
        cur.execute("SELECT Date,OP,HP,LP,TC,Vol,Amt FROM IndexData WHERE StkCode='{}'".format(benchMarkIndex))
        vals=cur.fetchall()
        dt = numpy.array(vals)
        dfi = pd.DataFrame(dt[:,1:].astype(numpy.float),index=dt[:,0],columns=['o','h','l','c','vol','amt'])
        data['index'+benchMarkIndex] = dfi
        
        stockUniverse = self.indexConstituents.GetAllStocks(self.rawDataStartDate,stkUniverIndex)
        if self.appendStockData == 1:
            for stk in stockUniverse:
                cur.execute("""SELECT Date,
                               (CASE WHEN Statu=-1 THEN OP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN HP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN LP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN TC_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN Vol ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN Amt ELSE Null END) 
                               FROM StockData WHERE StkCode='{}'""".format(stk))
                vals=cur.fetchall()
                if len(vals)>0:
                    _dt = numpy.array(vals)
                    _df = pd.DataFrame(_dt[:,1:].astype(numpy.float),index=_dt[:,0],columns=['o','h','l','c','vol','amt'])
                    data[stk] = _df       
        if self.appendFundData == 1:
            for stk in etfs:
                cur.execute("""SELECT Date,
                               (CASE WHEN Statu=-1 THEN OP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN HP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN LP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN TC_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN Vol ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN Amt ELSE Null END) 
                               FROM FundData WHERE StkCode='{}'""".format(stk))
                vals=cur.fetchall()
                #print type(vals[0][1])
                if len(vals)>0:
                    _dt = numpy.array(vals)
                    _df = pd.DataFrame(_dt[:,1:].astype(numpy.float),index=_dt[:,0],columns=['o','h','l','c','vol','amt'])
                    data[stk] = _df    
            
        self.dataPanel = pd.Panel(data)
        self.returns = pd.Panel({'r':self.dataPanel[:,:,'c'].pct_change()}).transpose(2,1,0)
        self.dataPanel = pd.concat([self.dataPanel,self.returns],axis=2)
        self.trdDays = self.dataPanel.major_axis.tolist()
        print self.trdDays
    
    #----------------------------------------------------------------------
    def CalcDateNDaysBefore(self,date,N):
        """"""
        pos = self.trdDays.index(date)-N+1
        return self.trdDays[pos]
    
    #----------------------------------------------------------------------
    def LoadScores(self, path):
        """"""
        scores = pd.read_csv(scoreFile,index_col=0)
        scores.index = score.index.astype('str')        
        
    #----------------------------------------------------------------------
    def Score2Signal(self,openThreshold,closeThreshold):
        """"""
        days = len(scores.index)
        num = len(scores.columns)
        scoreMat = scores.values
        signalMat = numpy.zeros((days,num))
        for d in range(2,days):
            for n in range(0,num):
                if signalMat[d-1,n]==0:
                    if scoreMat[d-1,n]>=openThreshold:
                        signalMat[d,n]=1
                if signalMat[d-1,n]==1:
                    if scoreMat[d-1,n]>=closeThreshold:
                        signalMat[d,n]=1

        self.signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
        return signals        
    
    #----------------------------------------------------------------------
    def CalcSignalReturn(self):
        """"""
        