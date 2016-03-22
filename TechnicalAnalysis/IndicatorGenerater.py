#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/3/7
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
class IndicatorGenerator(object):
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
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,OP_Adj,HP_Adj,LP_Adj,TC_Adj,Statu,Amt FROM MktData.A_Share_Data WHERE Date>='{}'".format(rawDataStartDate))            
        self.appendFundData = fundData            
        if fundData==1:
            cur.execute("CREATE TABLE FundData AS SELECT StkCode,Date,OP_Adj,HP_Adj,LP_Adj,TC_Adj,Statu,Amt FROM MktData.ExchangeTradedFund WHERE Date>='{}'".format(rawDataStartDate))
        cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,OP,HP,LP,TC,Amt FROM MktData.IndexData WHERE Date>='{}'".format(rawDataStartDate))        
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
        cur.execute("SELECT Date,OP,HP,LP,TC,Amt FROM IndexData WHERE StkCode='{}'".format(benchMarkIndex))
        vals=cur.fetchall()
        dt = numpy.array(vals)
        dfi = pd.DataFrame(dt[:,1:].astype(numpy.float),index=dt[:,0],columns=['o','h','l','c','amt'])
        data['index'+benchMarkIndex] = dfi
        
        stockUniverse = self.indexConstituents.GetAllStocks(self.rawDataStartDate,stkUniverIndex)
        if self.appendStockData == 1:
            for stk in stockUniverse:
                cur.execute("""SELECT Date,
                               (CASE WHEN Statu=-1 THEN OP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN HP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN LP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN TC_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN Amt ELSE Null END) 
                               FROM StockData WHERE StkCode='{}'""".format(stk))
                vals=cur.fetchall()
                if len(vals)>0:
                    _dt = numpy.array(vals)
                    _df = pd.DataFrame(_dt[:,1:].astype(numpy.float),index=_dt[:,0],columns=['o','h','l','c','amt'])
                    data[stk] = _df       
        if self.appendFundData == 1:
            for stk in etfs:
                cur.execute("""SELECT Date,
                               (CASE WHEN Statu=-1 THEN OP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN HP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN LP_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN TC_Adj ELSE Null END), 
                               (CASE WHEN Statu=-1 THEN Amt ELSE Null END) 
                               FROM FundData WHERE StkCode='{}'""".format(stk))
                vals=cur.fetchall()
                print type(vals[0][1])
                if len(vals)>0:
                    _dt = numpy.array(vals)
                    _df = pd.DataFrame(_dt[:,1:].astype(numpy.float),index=_dt[:,0],columns=['o','h','l','c','amt'])
                    data[stk] = _df    
            
        self.dataPenal = pd.Panel(data)
        print self.dataPenal['510050']
        #self.trdDay = self.df.index.tolist()
        
        
    #----------------------------------------------------------------------
    def Calc_IBS(self):
        """"""
        self.IBS = (self.dataPenal[:,:,'c']-self.dataPenal[:,:,'l'])/(self.dataPenal[:,:,'h']-self.dataPenal[:,:,'l'])
    
    #----------------------------------------------------------------------
    def Calc_CRSI(self,window):
        """"""
        priceDiff = self.dataPenal[:,:,'c'].diff()
        _gain = priceDiff[priceDiff>=0]
        print _gain
        _loss = -priceDiff[priceDiff<0]
        gain = _gain.fillna(0)
        loss = _loss.fillna(0)
        avgGain = pd.rolling_sum(gain,window)/float(window)
        avgLoss = pd.rolling_sum(loss,window)/float(window)+0.0000001
        rs = avgGain/avgLoss
        self.CRSI = 100-(100/(1+rs))
        

    
    #----------------------------------------------------------------------
    def TradeRules(self,b1,b2,b3):
        """"""
        days = len(self.CRSI.index)
        rsimat = self.CRSI.values
        ibsmat = self.IBS.values
        sigmat = numpy.zeros((days,2))
        for d in range(2,days):
            for n in range(0,2):
                if sigmat[d-1,n]==0:
                    if ibsmat[d-1,n]<=b1 and rsimat[d-1,n]<=b2:
                        sigmat[d,n]=1
                if sigmat[d-1,n]==1:
                    if ibsmat[d-1,n]<=0.6 and rsimat[d-1,n]<=b3:
                        sigmat[d,n]=1
        signals = pd.DataFrame(sigmat,index=self.CRSI.index,columns=self.CRSI.columns)
        signals.to_csv('signals.csv')
        _ret = self.dataPenal[:,:,'c'].pct_change()
        ret = _ret[signals==1]
        ret = ret.fillna(0)
        cumret = ret.cumsum()
        cumret.to_csv('cumret.csv')
        print cumret

        
        
        


    

        
if __name__ == "__main__":
    indicatorGenerator = IndicatorGenerator("MktData\\MktData_Wind_CICC.db", "20100127",0,1)
    indicatorGenerator.LoadDataIntoTimeSeries("000300","000300",["510050"])
    indicatorGenerator.Calc_IBS()
    indicatorGenerator.Calc_CRSI(3)
    indicatorGenerator.TradeRules(0.5,10,40)