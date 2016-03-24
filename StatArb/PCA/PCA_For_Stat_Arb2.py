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
        """
        Constructor
        """
        #Load raw data into in-memory database
        self.rawDataStartDate = rawDataStartDate
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

        #Get all stock symbol have been included in the target index 
        self.indexConstituents = InvestUniver.GetIndexCompStocks("MktGenInfo\\IndexComp_Wind_CICC.db")        
        
            
    #----------------------------------------------------------------------
    def LoadDataIntoTimeSeries(self,benchMarkIndex,stkUniverIndex,returnHorizon):
        """
        Load data from sql into python pandas data panel
        """
        #Index for hedging
        self.benchMarkIndex = benchMarkIndex
        #Stock universe
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
        self.histPriceDf = self.dp[:,:,'c']
        self.histPctRetDf = self.histPriceDf.pct_change()
        self.histLogPriceDf = numpy.log(self.histPriceDf)
        self.histLogRetDf = self.histLogPriceDf.diff()
        self.histVolDf = self.dp[:,:,'vol']
        
        
    #----------------------------------------------------------------------
    def GenEigenPort(self,date,v,percentage,sampleDays,extremeIndexRet):
        """
        Generate eigen vector portfolio
        """
        pos = self.trdDay.index(date)
        begSampleDate = self.trdDay[pos-sampleDays+1]
        endSampleDate = date
        if begSampleDate<=self.trdDay[0]:
            print "Not enough sample data for PCA, quit program"
            return None
        #去掉指数波动较大的交易日数据
        logRetDf = self.histLogRetDf[(self.histLogRetDf.index>=begSampleDate)*(self.histLogRetDf.index<=endSampleDate)]
        logRetDf = logRetDf.loc[numpy.abs(logRetDf['index'+self.benchMarkIndex])<extremeIndexRet]
        #获取当日指数成分股
        stockUniverse = self.indexConstituents.GetStocks(date,self.stkUniverIndex)
        stockUniverse.sort()
        logRetDf = logRetDf[stockUniverse]
        #去掉停牌天数较多的股票
        effectiveStkNum = int(len(logRetDf.index)*0.75)
        for stk in logRetDf.columns:
            if logRetDf[stk].isnull().sum()>(len(logRetDf.index)-effectiveStkNum):
                logRetDf=logRetDf.drop(stk,axis=1)
        #标准化收益率
        self.stkStdTs = logRetDf.std()
        logRetDf = (logRetDf-logRetDf.mean())/self.stkStdTs     
        logRetDf = logRetDf.fillna(0)
        #获得最后用于计算主成分的股票
        selectedStock = logRetDf.columns.tolist()
        stkNum = len(selectedStock)
        corrMatDf = logRetDf.cov()
        mat = corrMatDf.values
        #Shrinkage the correlation matrix 
        _mat = numpy.zeros((stkNum,stkNum))
        for i in xrange(stkNum):
            for j in xrange(stkNum):
                if i==j:
                    _mat[i,j]=mat[i,j]
                else:
                    _mat[i,j]=mat[i,j]/(1+10**-9)
        #Eigen value decompsition
        eig = numpy.linalg.eigh(_mat)
        eigVec = eig[1]
        eigVecCols = numpy.linspace(stkNum-1,0,stkNum)
        eigVecIndex = selectedStock
        self.eigVecDf = pd.DataFrame(eigVec,index=eigVecIndex,columns=eigVecCols)
        #self.eigVecDf.to_csv('eigVec.csv')
        eigVals = eig[0][::-1]
        self.eigVals = eigVals
        self.cumSumEigVals = numpy.cumsum(eigVals)
        sumEigVals = self.cumSumEigVals[-1]
        #for item in self.cumSumEigVals:
        #    print item/sumEigVals
        #Get the significant eigen vector number
        for i in xrange(stkNum):
            if self.cumSumEigVals[i+1]/sumEigVals>=percentage:
                break
        if v==0:
            self.significantEigNum = i+1
        else:
            self.significantEigNum = v
        self.sigEigVals = self.eigVals[0:self.significantEigNum]
        self.eigenPortStock = selectedStock
        return date,i+1,self.histPctRetDf['index'+self.benchMarkIndex][date],eigVals[0]/sumEigVals,len(logRetDf.index)

    #----------------------------------------------------------------------
    def CalcEigenPortRet(self,begDate,endDate,winsorize):
        """
        计算主成分股票组合收益
        """
        #用于计算主成分组合收益和随后进行回归计算的股票收益数据
        logRetDf = self.histLogRetDf[(self.histLogRetDf.index>=begDate)*(self.histLogRetDf.index<=endDate)][self.eigenPortStock]
        #用于计算主成分组合收益和随后进行回归计算的股票收益数据,待去除极值
        _logRetDf = logRetDf.copy()
        #数据去除极值
        if winsorize!=0:
            m = _logRetDf.mean()
            s = _logRetDf.std()
            ub = m+winsorize*s
            lb = m-winsorize*s
            ubb = numpy.abs(numpy.sign(_logRetDf))*ub
            lbb = numpy.abs(numpy.sign(_logRetDf))*lb
            _logRetDf[_logRetDf>ub]=ubb
            _logRetDf[_logRetDf<lb]=lbb
        _logRetDf = _logRetDf.fillna(0)
        logRetDf = logRetDf.fillna(0)
        self.logRetDf = logRetDf#用于计算主成分组合收益和随后进行回归计算的股票收益数据
        self.logRetTrimedDf = _logRetDf#用于计算主成分组合收益和随后进行回归计算的股票收益数据，已经去除极值
        self.volDf = self.histVolDf[(self.histVolDf.index>=begDate)*(self.histVolDf.index<=endDate)][self.eigenPortStock]
        stkStdTs = self.stkStdTs
        #Here is to compute significant eigen portfolio return
        sigEigDf = self.eigVecDf[range(self.significantEigNum)]
        wgt = sigEigDf/numpy.sqrt(self.sigEigVals)
        wgtMat = wgt.values
        stkRetAdjDf = self.logRetDf/stkStdTs
        stkRetAdjMat = stkRetAdjDf.values
        eigenPortRetMat = numpy.dot(stkRetAdjMat,wgtMat)
        self.eigenPortRetDf = pd.DataFrame(eigenPortRetMat,index=self.logRetDf.index,columns=range(self.significantEigNum))
        
        
    #----------------------------------------------------------------------
    def RegressOnEigenFactor(self,date,sampleDays,winsorize=0):
        """
        Regress stock return on eigen portfolio return
        """
        pos = self.trdDay.index(date)
        begDate = self.trdDay[pos-sampleDays+1]
        endDate = date
        #Calculate eigen portfolio return
        self.CalcEigenPortRet(begDate,endDate,winsorize)
        #vector to store residuals
        o = numpy.ones([len(self.eigenPortRetDf.index),1])
        A = numpy.hstack((self.eigenPortRetDf.values,o))
        y = self.logRetTrimedDf.values
        #最小二乘法回归
        res = numpy.linalg.lstsq(A,y)
        #计算回归残差
        residual = y - numpy.dot(A,res[0])
        residualDf = pd.DataFrame(residual,index=self.logRetTrimedDf.index,columns=self.eigenPortStock)
        #计算R-Square
        rss = sum(residual**2)
        tss = sum((y-numpy.mean(y,0))**2)
        m = A.shape[0]
        n = A.shape[1]-1
        std_error = numpy.sqrt(rss/(m-n))
        std_y = numpy.sqrt(tss/(m-1))
        r2Adj = 1-(std_error/std_y)**2
        self.regressR2Adj = pd.Series(r2Adj,index=self.eigenPortStock)
        #resiDf.cumsum().to_csv('residuals2.csv')
        #self.regressR2Adj.to_csv('r2_adj.csv')
        #print t2-t1,t3-t2    
        self.regressResidualDf = residualDf
        
    
    #----------------------------------------------------------------------
    def OUFitAndCalcZScore(self,sampleDays,VolumeAdjusted,drift):
        """
        O-U fit on regression residuals 
        """
        if VolumeAdjusted==1:
            wgt = self.volDf.mean()/(self.volDf)
            wgt = wgt.replace(numpy.inf,numpy.nan)
            wgt = wgt.fillna(0)
            residual = self.regressResidualDf*wgt
        else:
            residual = self.regressResidualDf
        #self.regressResiduals.cumsum().to_csv("cumresidual.csv")
        res = residual.iloc[-sampleDays:].cumsum().apply(CalcZScore,args=(drift,))
        #res.to_csv('scors.csv')
        score = (res.loc['m']-res.loc['m'].mean())/res.loc['s']
        score = score.to_frame('score_adj')
        res = res.append(score.transpose())
        return res,self.regressR2Adj
         
         
    #----------------------------------------------------------------------
    def SimpleBacktest(self,scores,revsT,openThreshold,closeThreshold,revsThreshold,rSqrd,dd):
        """"""
        startDate = scores.index[0]
        endDate = scores.index[-1]
        dfRet = self.histPctRetDf[(self.histPctRetDf.index>=startDate)*(self.histPctRetDf.index<=endDate)]
        signals = TradeRule1(scores,revsT,openThreshold,closeThreshold,revsThreshold,dd)
        signals.to_csv("signals.csv")
        trdTime = signals.diff().abs().sum(axis=1)
        #trdTime.to_csv('tradeTimes.csv')
        posi = signals.sum(axis=1)
        longPorts = dfRet.where(signals==1)
        #longPorts.to_csv("LongPorts.csv")
        #shortPorts.to_csv("ShortPorts.csv")
        longRets = longPorts.mean(1)
        shortRets = dfRet['index'+self.benchMarkIndex]
        hedgeRet = longRets - shortRets
        return hedgeRet,posi,trdTime,longRets
        #hedgeRet.to_csv("HedgeRets.csv")


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
    

        

    
