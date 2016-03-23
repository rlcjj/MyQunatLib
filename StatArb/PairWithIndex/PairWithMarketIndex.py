#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/3/23
"""

import os,sys,logging ,time,decimal,codecs,numpy 
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import numpy as np
import pandas as pd
import DefineInvestUniverse.GetIndexCompStocks as InvestUniver   
from pandas.stats.api import ols
from scipy import linalg


########################################################################
class PairWithMarketIndex(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,mktDataDbName,ifLoadMemoryDb,rawDataStartDate):
        """Constructor"""
        mktDbPath = GetPath.GetLocalDatabasePath()["RawEquity"]+mktDataDbName
        if ifLoadMemoryDb==1:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            print "Load market data into memory database"
            cur.execute("ATTACH '{}' AS MktData".format(mktDbPath))
            cur.execute("CREATE TABLE AStockData AS SELECT StkCode,Date,TC_Adj,Vol,Statu FROM MktData.AStockData WHERE Date>='{}'".format(rawDataStartDate))
            cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,TC,Vol FROM MktData.IndexData WHERE Date>='{}'".format(rawDataStartDate))
            print "Done"
            print "Create index on table StockData and table IndexData"
            cur.execute("CREATE INDEX idS ON AStockData (StkCode,Date)")
            cur.execute("CREATE INDEX idI ON IndexData (StkCode,Date)")
            print "Done"
        else:
            print "Directly connect to local database"
            self.conn = lite.connect(mktDbPath)
            self.conn.text_factory = str
            
        self.indexConstituents = InvestUniver.GetIndexCompStocks("MktGenInfo\\IndexComp_Wind_CICC.db")        
        self.rawDataStartDate = rawDataStartDate        
    
    
    #----------------------------------------------------------------------
    def LoadDataIntoTimeSeries(self,benchMarkIndex,stkUniverIndex,returnHorizon):
        """"""
        self.benchMarkIndex = benchMarkIndex
        self.stkUniverIndex = stkUniverIndex
        
        stockUniverse = self.indexConstituents.GetAllStocks(self.rawDataStartDate,stkUniverIndex)
        
        data = {}
        
        cur = self.conn.cursor()
        cur.execute("SELECT Date,TC,Vol FROM IndexData WHERE StkCode='{}'".format(benchMarkIndex))
        vals=cur.fetchall()
        dt = numpy.array(vals)
        dfi = pd.DataFrame(dt[:,1:].astype(numpy.float),index=dt[:,0],columns=['c','vol'])
        data['index'+benchMarkIndex] = dfi
        
        for stk in stockUniverse:
            cur.execute("""SELECT Date,
                                  (CASE WHEN Statu=-1 THEN TC_Adj ELSE Null END),
                                  (CASE WHEN Statu=-1 THEN Vol ELSE Null END)
                                   FROM AStockData WHERE StkCode='{}'""".format(stk))
            vals=cur.fetchall()
            if len(vals)>0:
                _dt = numpy.array(vals)
                #print stk
                _df = pd.DataFrame(_dt[:,1:].astype(numpy.float),index=_dt[:,0],columns=['c','vol'])     
                data[stk] = _df
            
        self.dp = pd.Panel(data)
        self.trdDay = self.dp.major_axis.tolist()
        self.histClosePrice = self.dp[:,:,'c']
        self.histPctRet = self.histClosePrice.pct_change()
        self.histLogPrice = numpy.log(self.histClosePrice)
        self.histLogRet = self.histLogPrice.diff()
       
        
    #----------------------------------------------------------------------
    def RegressOnIndex(self,date,sampleDays,extreIndexVal,winsorize=0):
        """"""   
        sampleBegDatePosi = self.trdDay.index(date)
        sampleBegDate = self.trdDay[sampleBegDatePosi-sampleDays+1]
        sampleEndDate = date         
        _sampleRet = self.histLogRet[(self.histLogRet.index>=sampleBegDate)*(self.histLogRet.index<=sampleEndDate)]
        sampleRet = _sampleRet.loc[np.abs(_sampleRet["index"+self.benchMarkIndex])<extreIndexVal]
        indexRet = sampleRet[["index"+self.benchMarkIndex]]
        stockUniverse = self.indexConstituents.GetStocks(date,self.stkUniverIndex)
        stockUniverse.sort()  
        stkRet = sampleRet[stockUniverse]
        effectiveStkNum = int(len(sampleRet.index)*0.6)
        for stk in stkRet.columns:
            if stkRet[stk].isnull().sum()>(len(stkRet.index)-effectiveStkNum):
                stkRet=stkRet.drop(stk,axis=1)       
        self.selectedStk = stkRet.columns.tolist()
        stkRet = stkRet.fillna(0)
        
        if winsorize!=0:
            m = stkRet.mean()
            s = stkRet.std()
            ub = m+winsorize*s
            lb = m-winsorize*s
            ubb = np.abs(np.sign(stkRet))*ub
            lbb = np.abs(np.sign(stkRet))*lb
            stkRet[stkRet>ub]=ubb
            stkRet[stkRet<lb]=lbb  
            
        ones = np.ones([len(stkRet.index),1])
        x = np.hstack((indexRet.values,ones))
        y = stkRet.values
            
        self.regression = numpy.linalg.lstsq(x,y)
        #resi = y - numpy.dot(x,self.regression[0])
        #rss = sum(resi**2)
        #tss = sum((y-numpy.mean(y,0))**2)
        #m = x.shape[0]
        #n = x.shape[1]-1
        #std_error = numpy.sqrt(rss/(m-n))
        #std_y = numpy.sqrt(tss/(m-1))
        #r2Adj = 1-(std_error/std_y)**2
        #self.regressR2Adj = pd.Series(r2Adj,index=self.eigPortUniver)
        
        
    #----------------------------------------------------------------------
    def OUFitAndCalcZScore(self,date,sampleDays,drift):
        """"""
        sampleBegDatePosi = self.trdDay.index(date)
        sampleBegDate = self.trdDay[sampleBegDatePosi-sampleDays+1]
        sampleEndDate = date         
        sampleRet = self.histLogRet[(self.histLogRet.index>=sampleBegDate)*(self.histLogRet.index<=sampleEndDate)]  
        stkRet = sampleRet[self.selectedStk]
        stkRet = stkRet.fillna(0)
        indexRet = sampleRet[["index"+self.benchMarkIndex]]
        ones = np.ones([len(stkRet.index),1])
        x = np.hstack((indexRet.values,ones))
        _residue = stkRet.values - np.dot(x,self.regression[0])
        residue = pd.DataFrame(_residue,index=indexRet.index,columns=self.selectedStk)
        
        res = residue.cumsum().apply(CalcZScore,args=(drift,))
        score = (res.loc['m']-res.loc['m'].mean())/res.loc['s']
        score = score.to_frame('score_adj')
        res = res.append(score.transpose())
        return res      
    
    #----------------------------------------------------------------------
    def SimpleBacktest(self,scores,revsT,openThreshold,closeThreshold,revsThreshold,rSqrd,dd):
        """"""
        startDate = scores.index[0]
        endDate = scores.index[-1]
        stkRet = self.histPctRet[(self.retDf.index>=startDate)*(self.retDf.index<=endDate)]
        signals = TradeRule1(scores,revsT,openThreshold,closeThreshold,revsThreshold,dd)
        signals.to_csv("signals.csv")
        trdTime = signals.diff().abs().sum(axis=1)
        trdTime.to_csv('tradeTimes.csv')
        posi = signals.sum(axis=1)
        longPorts = stkRet.where(signals==1)
        #longPorts.to_csv("LongPorts.csv")
        #shortPorts.to_csv("ShortPorts.csv")
        longRets = longPorts.mean(1)
        shortRets = dfRet['index'+self.benchMarkIndex]
        hedgeRet = longRets - shortRets
        return hedgeRet,posi,longRets
        #hedgeRet.to_csv("HedgeRets.csv")    
    
    
    
#----------------------------------------------------------------------
def CalcZScore(_x,drift):
    """"""
    l = len(_x)
    xm = numpy.mean(_x)
    start = float(xm/l)
    if drift==1:
        tr = numpy.linspace(start,xm,l)
        x = (numpy.array(_x)-tr).tolist()
    else:
        x = _x
    y = x[1:]
    X = numpy.vstack((x[0:-1],numpy.ones(len(x)-1))).T
    res = numpy.linalg.lstsq(X,y)
    a = res[0][1]
    b = res[0][0]
    r = y - numpy.dot(X,res[0])
    v = numpy.var(r)
    k = -numpy.log(b)*250
    m = x[-1]-a/(1-b)
    sig = numpy.sqrt(v*2*k/(1-b**2))
    sigEq = numpy.sqrt(v/(1-b**2))
    s = m/sigEq
    #print s
    #print k
    return pd.Series({'m':m,'s':sigEq,'period':250/k,'score':s})


#----------------------------------------------------------------------
def TradeRule1(scores,revsT,openThreshold,closeThreshold,revsThreshold,dd):
    """"""
    days = len(scores.index)
    num = len(scores.columns)
    scoreMat = scores.values
    revsTMat = revsT.values
    signalMat = numpy.zeros((days,num))
    for d in range(2,days):
        for n in range(0,num):
            if signalMat[d-1,n]==0:
                if (scoreMat[d-1-dd,n]<-openThreshold and revsTMat[d-1-dd,n]<revsThreshold):
                    signalMat[d,n]=1
            if signalMat[d-1,n]==1 and revsTMat[d-1-dd,n]<revsThreshold:
                if scoreMat[d-1-dd,n]<-closeThreshold:
                    signalMat[d,n]=1
    signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
    return signals
    

#----------------------------------------------------------------------
def TradeRule2(scores,revsT,openThreshold,closeThreshold,revsThreshold):
    """"""
    dd=0
    days = len(scores.index)
    num = len(scores.columns)
    scoreMat = scores.values
    revsTMat = revsT.values
    signalMat = numpy.zeros((days,num))
    for d in range(2,days):
        for n in range(0,num):
            if signalMat[d-1,n]==0:
                if (scoreMat[d-2-dd,n]<scoreMat[d-1-dd,n] and scoreMat[d-1-dd,n]<=-openThreshold and revsTMat[d-1-dd,n]<revsThreshold and (0.333*scoreMat[d-3-dd,n]+0.667*scoreMat[d-3-dd,n])/1.0<scoreMat[d-1-dd,n]):
                    signalMat[d,n]=1
            if signalMat[d-1,n]==1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]<-closeThreshold:
                    signalMat[d,n]=1
    signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
    return signals
    
#----------------------------------------------------------------------
def TradeRule3(scores,revsT,openThreshold,closeThreshold,revsThreshold,dd):
    """"""
    days = len(scores.index)
    num = len(scores.columns)
    scoreMat = scores.values
    revsTMat = revsT.values
    signalMat = numpy.zeros((days,num))
    for d in range(2,days):
        for n in range(0,num):
            if signalMat[d-1,n]==0:
                if (scoreMat[d-2-dd,n]<scoreMat[d-1-dd,n] and scoreMat[d-1-dd,n]<=-openThreshold and revsTMat[d-1-dd,n]<revsThreshold):
                    signalMat[d,n]=1
            if signalMat[d-1,n]==1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]<-closeThreshold:
                    signalMat[d,n]=1
    signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
    return signals