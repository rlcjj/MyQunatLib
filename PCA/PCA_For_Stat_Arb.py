#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/2/17
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
class PCA_For_Stat_Arb(object):
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
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,TC_Adj,Statu FROM MktData.A_Share_Data WHERE Date>='{}'".format(rawDataStartDate))
            cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,TC FROM MktData.IndexData WHERE Date>='{}'".format(rawDataStartDate))
            print "Done"
            print "Create index on table StockData and table IndexData"
            cur.execute("CREATE INDEX idS ON StockData (StkCode,Date)")
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
        
        cur = self.conn.cursor()
        cur.execute("SELECT Date,TC FROM IndexData WHERE StkCode='{}'".format(benchMarkIndex))
        vals=cur.fetchall()
        dt = numpy.array(vals)
        dfi = pd.DataFrame(dt[:,1].astype(numpy.float),index=dt[:,0],columns=['index'+benchMarkIndex])
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
        self.trdDay = self.df.index.tolist()
        self.df = self.df[::returnHorizon]
        self.retDf = self.df.pct_change()
        #self.retDf = numpy.log(self.df)
        #self.retDf = self.retDf.diff()
        
    #----------------------------------------------------------------------
    def GenEigenPort(self,date,v,percentage,nSampleDate,outlier):
        """"""
        pos = self.trdDay.index(date)
        #print pos
        startDate = self.trdDay[pos-nSampleDate+1]
        if startDate<=self.trdDay[0]:
            print "Not enough sample data for PCA, quit program"
            return None
        dfRet = self.retDf[(self.retDf.index>=startDate)*(self.retDf.index<=date)]
        dfRet = dfRet.loc[dfRet['index'+self.benchMarkIndex]>-outlier]
        dfRet = dfRet.loc[dfRet['index'+self.benchMarkIndex]<outlier]
        dfRet = (dfRet-dfRet.mean())/dfRet.std()
        self.stkStd = dfRet.std()
        stockUniverse = self.indexConstituents.GetStocks(date,self.stkUniverIndex)
        stockUniverse.sort()
        effectiveNum = int(len(dfRet.index)*0.4)
        dfRet = dfRet[stockUniverse]
        #dfRet.to_csv('ret.csv')
        for stk in dfRet.columns:
            if dfRet[stk].isnull().sum()>(len(dfRet.index)-effectiveNum):
                dfRet=dfRet.drop(stk,axis=1)
        #dfRet.to_csv('ewt2.csv')
        refinedStkUniver = dfRet.columns.tolist()
        stkNum = len(refinedStkUniver)
        corrMat = dfRet.cov()
        corrMat = corrMat.fillna(0)
        mat = corrMat.values
        _mat = numpy.zeros((stkNum,stkNum))
        for i in xrange(stkNum):
            for j in xrange(stkNum):
                if i==j:
                    _mat[i,j]=mat[i,j]
                else:
                    _mat[i,j]=mat[i,j]/(1+10**-9)
        eig = numpy.linalg.eigh(_mat)
        eigVec = eig[1]
        eigVecCols = numpy.linspace(stkNum-1,0,stkNum)
        eigVecIndex = refinedStkUniver
        #print eigVecIndex
        #print eigVecCols
        self.eigVecDf = pd.DataFrame(eigVec,index=eigVecIndex,columns=eigVecCols)
        #self.eigVecDf.to_csv('eigVec.csv')
        eigVals = eig[0][::-1]
        self.eigVals = eigVals
        self.cumSumEigVals = numpy.cumsum(eigVals)
        sumEigVals = self.cumSumEigVals[-1]
        #for item in self.cumSumEigVals:
        #    print item/sumEigVals
        for i in xrange(stkNum):
            if self.cumSumEigVals[i+1]/sumEigVals>=percentage:
                break
        if v==0:
            self.significantEigNum = i
        else:
            self.significantEigNum = v
        self.sigEigVals = self.eigVals[0:self.significantEigNum]
        self.eigPortUniver = refinedStkUniver
        return date,i+1,self.df['index'+self.benchMarkIndex][date],eigVals[0]/sumEigVals,len(dfRet.index)

    #----------------------------------------------------------------------
    def CalcEigenPortRet(self,startDate,endDate,winsorize):
        """"""
        _stkRetDf = self.retDf[(self.retDf.index>=startDate)*(self.retDf.index<=endDate)]
        
        if winsorize!=0:
            m = _stkRetDf.mean()
            s = _stkRetDf.std()
            ub = m+3*s
            lb = m-3*s
            ubb = numpy.sign(_stkRetDf)*ub
            lbb = numpy.sign(_stkRetDf)*lb
            ubMsk = _stkRetDf>ub
            lbmsk = _stkRetDf<lb
            _stkRetDf.where(_stkRetDf>ub,ubb)
            _stkRetDf.where(_stkRetDf<lb,lbb)
        _stkRetDf = _stkRetDf.fillna(0)
        self.stkRetDf = _stkRetDf[self.eigPortUniver]
        stkStd = self.stkStd[self.eigPortUniver]
        
        #=============================================================
        #Here is to compute significant eigen portfolio return
        #=============================================================
        sigEigDf = self.eigVecDf[range(self.significantEigNum)]
        wgt = sigEigDf/numpy.sqrt(self.sigEigVals)
        wgtMat = wgt.values
        stkRetAdj = self.stkRetDf/stkStd
        stkRetAdjMat = stkRetAdj.values
        eigPortRetMat = numpy.dot(stkRetAdjMat,wgtMat)
        self.eigPortRet = pd.DataFrame(eigPortRetMat,index=self.stkRetDf.index,columns=range(self.significantEigNum))
        
        #=============================================================
        #Alternative method to compute significant eigen portfolio returns
        #=============================================================
        #portRet = []
        #for i in xrange(self.significantEigNum):
            #_portRet = 0 
            #w = 0
            #for stk in self.eigPortUniver:
                #w += self.eigVecDf[i][stk]/self.stkStd[stk]
            #for stk in self.eigPortUniver:
                ##print self.eigDf[i][stk]
                #_portRet += stkRetDf[stk]*self.eigVecDf[i][stk]/(self.stkStd[stk]*numpy.sqrt(self.eigVals[i]))
                ##print _portRet
            #portRet.append(_portRet.to_frame(i))
        #self.eigPortRet = pd.concat(portRet,axis=1)

        #self.eigPortRet.to_csv('eigenPortRetNew.csv')
        self.eigPortRetStartDate = startDate
        self.eigPortRetEndDate = endDate
        
    #----------------------------------------------------------------------
    def RegressOnEigenFactor(self,date,nSampleDate,winsorize=0):
        """"""
        pos = self.trdDay.index(date)
        #print pos
        startDate = self.trdDay[pos-nSampleDate+1]
        t1 = time.time()
        self.CalcEigenPortRet(startDate, date, winsorize)
        t2 = time.time()
        resiDfVec = []
        o = numpy.ones([len(self.eigPortRet.index),1])
        A = numpy.hstack((self.eigPortRet.values,o))
        y = self.stkRetDf.values
        #print A
        #print y
        res = numpy.linalg.lstsq(A,y)
        """
        for stk in self.eigPortUniver:
            res = ols(y=self.stkRetDfstk],x=self.eigPortRet)
            resiDfVec.append(res.resid.to_frame(stk))
        resiDf = pd.concat(resiDfVec,axis=1)
        """
        resi = y - numpy.dot(A,res[0])
        resiDf = pd.DataFrame(resi,index=self.eigPortRet.index,columns=self.eigPortUniver)
        #for stk in self.eigPortUniver:
        #    print stk 
        #resiDf.to_csv('residuals.csv')
        rss = sum(resi**2)
        tss = sum((y-numpy.mean(y,0))**2)
        m = A.shape[0]
        n = A.shape[1]-1
        std_error = numpy.sqrt(rss/(m-n))
        std_y = numpy.sqrt(tss/(m-1))
        r2Adj = 1-(std_error/std_y)**2
        self.regressR2Adj = pd.Series(r2Adj,index=self.eigPortUniver)
        t3 = time.time()
        #resiDf.cumsum().to_csv('residuals2.csv')
        #self.regressR2Adj.to_csv('r2_adj.csv')
        #print t2-t1,t3-t2    
        self.regressResiduals = resiDf
        
    
    #----------------------------------------------------------------------
    def OUFitAndCalcZScore(self,sampleDays,drift):
        """"""
        #self.regressResiduals.cumsum().to_csv("cumresidual.csv")
        res = self.regressResiduals.iloc[-sampleDays:].cumsum().apply(CalcZScore,args=(drift,))
        #res.to_csv('scors.csv')
        score = (res.loc['m']-res.loc['m'].mean())/res.loc['s']
        score = score.to_frame('score_adj')
        res = res.append(score.transpose())
        return res,self.regressR2Adj
         
         
    #----------------------------------------------------------------------
    def SimpleBacktest(self,scores,revsT,openThreshold,closeThreshold,revsThreshold,rSqrd):
        """"""
        startDate = scores.index[0]
        endDate = scores.index[-1]
        dfRet = self.retDf[(self.retDf.index>=startDate)*(self.retDf.index<=endDate)]
        signals = TradeRule3(scores,revsT,openThreshold,closeThreshold,revsThreshold)
        signals.to_csv("signals.csv")
        longPorts = dfRet.where(signals==1)
        shortPorts = dfRet.where(signals==-1)
        #longPorts.to_csv("LongPorts.csv")
        #shortPorts.to_csv("ShortPorts.csv")
        longRets = longPorts.mean(1)
        shortRets = shortPorts.mean(1)
        shortRets = dfRet['index000300']
        hedgeRet = longRets - shortRets
        return hedgeRet
        #hedgeRet.to_csv("HedgeRets.csv")


#----------------------------------------------------------------------
def TradeRule1(scores,revsT,openThreshold,closeThreshold,revsThreshold):
    """"""
    days = len(scores.index)
    num = len(scores.columns)
    scoreMat = scores.values
    revsTMat = revsT.values
    signalMat = numpy.zeros((days,num))
    for d in range(2,days):
        for n in range(0,num):
            if signalMat[d-1,n]==0:
                if (scoreMat[d-2,n]<-openThreshold and scoreMat[d-1,n]>=-openThreshold and scoreMat[d-1,n]<-closeThreshold and revsTMat[d-1,n]<revsThreshold):
                    signalMat[d,n]=1
                if (scoreMat[d-2,n]>openThreshold and scoreMat[d-1,n]<=openThreshold and scoreMat[d-1,n]>closeThreshold and revsTMat[d-1,n]<revsThreshold):
                    signalMat[d,n]=-1
            if signalMat[d-1,n]==1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]<-closeThreshold:
                    signalMat[d,n]=1
            if signalMat[d-1,n]==-1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]>closeThreshold:
                    signalMat[d,n]=-1
    signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
    return signals
    

#----------------------------------------------------------------------
def TradeRule2(scores,revsT,openThreshold,closeThreshold,revsThreshold):
    """"""
    days = len(scores.index)
    num = len(scores.columns)
    scoreMat = scores.values
    revsTMat = revsT.values
    signalMat = numpy.zeros((days,num))
    for d in range(2,days):
        for n in range(0,num):
            if signalMat[d-1,n]==0:
                if (scoreMat[d-2,n]<scoreMat[d-1,n] and scoreMat[d-1,n]<=-openThreshold and revsTMat[d-1,n]<revsThreshold and (scoreMat[d-3,n]+scoreMat[d-3,n])/2.0<scoreMat[d-1,n]):
                    signalMat[d,n]=1
                if (scoreMat[d-2,n]>scoreMat[d-1,n] and scoreMat[d-1,n]>=openThreshold and revsTMat[d-1,n]<revsThreshold and (scoreMat[d-3,n]+scoreMat[d-3,n])/2.0>scoreMat[d-1,n]):
                    signalMat[d,n]=-1
            if signalMat[d-1,n]==1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]<-closeThreshold:
                    signalMat[d,n]=1
            if signalMat[d-1,n]==-1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]>closeThreshold:
                    signalMat[d,n]=-1
    signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
    return signals
    
#----------------------------------------------------------------------
def TradeRule3(scores,revsT,openThreshold,closeThreshold,revsThreshold):
    """"""
    dd=1
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
                if (scoreMat[d-2-dd,n]>scoreMat[d-1-dd,n] and scoreMat[d-1-dd,n]>=openThreshold and revsTMat[d-1-dd,n]<revsThreshold):
                    signalMat[d,n]=-1
            if signalMat[d-1,n]==1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1-dd,n]<-closeThreshold:
                    signalMat[d,n]=1
            if signalMat[d-1,n]==-1 and revsTMat[d-1,n]<revsThreshold:
                if scoreMat[d-1,n]>closeThreshold:
                    signalMat[d,n]=-1
    signals = pd.DataFrame(signalMat,index=scores.index,columns=scores.columns)
    return signals
    

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
    

        

if __name__ == "__main__":    
    tm1 = time.time()
    corrMatCalc = PCA_For_Stat_Arb("MktData\\MktData_Wind_CICC.db", 1,"20100127")
    corrMatCalc.LoadDataIntoTimeSeries("000300",1)
    tm2 = time.time()
    print tm2-tm1
    corrMatCalc.GenEigenPort("20131217",0,0.7,250,0.05)
    corrMatCalc.RegressOnEigenFactor('20131217', 60,winsorize=0)
    res = corrMatCalc.OUFitAndCalcZScore()
    res[0].to_csv('scores.csv')
    
    #corrMatCalc.CalEigenPortRet("20150121", "20160128")
    
    
    #x = numpy.array([numpy.sin(numpy.array(range(0,60))/10.0)+numpy.random.rand(60),
                    #numpy.sin(numpy.array(range(0,60))/10.0)+numpy.random.rand(60),
                    #numpy.sin(numpy.array(range(0,60))/10.0)+numpy.random.rand(60)]).T
    #df = pd.DataFrame(x,index=range(0,60),columns=['b ','a','c'])

    
    #res2 = df.apply(CalcZScore)
    #res2.to_csv('pdTest.csv')
    #print res2
    
    #x = numpy.array([1,2,3,4,5,6,6,7,7,8,8,9,9,90,0,0])
    #y = numpy.zeros(len(x))
    #X = numpy.vstack((x,numpy.ones(len(x)))).T
    #res = numpy.linalg.lstsq(X,y)
    #print "done"
            
        
    