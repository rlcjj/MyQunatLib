#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/3/9
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
    def SetStockUniverse(self,date):
        """"""
        self.newStkUniverByGivenDate = self.indexConstituents.GetStocks(date,self.stkUniverIndex)
        self.dataPenalByGivenDate = self.dataPanel[self.newStkUniverByGivenDate]
        #print self.dataPenal
        
    
    #----------------------------------------------------------------------
    def Calc_Indicator1(self,date):
        """
        Alpha#1: (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) -0.5)
        """
        dateBeg= self.CalcDateNDaysBefore(date,20+5)
        data = self.dataPanel[:,dateBeg:date,:]
        std = pd.rolling_std(data[:,:,'r'],20)
        std2 = numpy.power(std,2)
        cp2 = numpy.power(data[:,:,'c'],2)
        ret = data[:,:,'r']
        cp2[ret<0]=std2
        #print cp2
        _sig = cp2.iloc[-5:].set_index([[4,3,2,1,0]]).apply(numpy.argmax)
        cp2.iloc[-5:].set_index([[4,3,2,1,0]]).to_csv('ret.csv')
        _sig.to_csv('sig.csv')
        sig = _sig.rank(pct=True)-0.5
        sig.to_csv('alpha1.csv')
        return sig 
        
    #----------------------------------------------------------------------
    def Calc_Indicator2(self,date,**params):
        """
        Alpha#2: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
        """
        t1 = params["t1"]
        t2 = params["t2"]
        dateBeg= self.CalcDateNDaysBefore(date,t1+t2)
        data = self.dataPanel[:,dateBeg:date,:]
        volLog = numpy.log(data[:,:,'amt'])
        retCO = (data[:,:,'c']-data[:,:,'o'])/data[:,:,'o']
        #data[:,:,'c'].to_csv('c.csv')
        #data[:,:,'c'].diff(2).to_csv('c1.csv')
        #data[:,:,'o'].to_csv('o.csv')
        #retCO.to_csv('r.csv')
        deltaVolLog = volLog.diff(t2)
        vRank = deltaVolLog.rank(axis=1,pct=True).iloc[-t1:]
        rRank = retCO.rank(axis=1,pct=True).iloc[-t1:]
        #vRank.to_csv('vr.csv')
        #rRank.to_csv('rr.csv')
        corr = vRank.corrwith(rRank)
        #corr.to_csv('corr.csv')
        sig = -corr
        return sig
    
    #----------------------------------------------------------------------
    def Calc_Indicator3(self,date,**params):
        """
        Alpha#3: (-1 * correlation(rank(open), rank(volume), 10))
        """
        t1 = params["t1"]
        dateBeg= self.CalcDateNDaysBefore(date,t1)
        data = self.dataPanel[:,dateBeg:date,:]
        op = data[:,:,'o'].iloc[-t1:]
        vol = data[:,:,'amt'].iloc[-t1:]
        opRank = op.rank(axis=1,pct=True)
        volRank = vol.rank(axis=1,pct=True)
        corr = opRank.corrwith(volRank)
        sig = -corr
        return sig
    
    #----------------------------------------------------------------------
    def Calc_Indicator4(self,date,**params):
        """
        Alpha#4: (-1 * Ts_Rank(rank(low), 9))
        """
        t1 = params["t1"]
        dateBeg= self.CalcDateNDaysBefore(date,t1)
        data = self.dataPanel[:,dateBeg:date,:]        
        low = data[:,:,'l'].iloc[-t1:]
        tsRank = low.rank(axis=0,pct=True)
        sig = -tsRank
        return sig
    
    #----------------------------------------------------------------------
    def Calc_Indicator5(self):
        """
        Alpha#5: (rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))
        """
        print "No data!"
        
    #----------------------------------------------------------------------
    def Calc_Indicator6(self,date,**params):
        """
        Alpha#6: (-1 * correlation(open, volume, 10))
        """
        t1 = params["t1"]
        dateBeg= self.CalcDateNDaysBefore(date,t1)
        data = self.dataPanel[:,dateBeg:date,:]
        op = data[:,:,'o'].iloc[-t1:]
        vol = data[:,:,'amt'].iloc[-t1:]
        corr = op.corrwith(vol)
        sig = -corr
        return sig
    
    #----------------------------------------------------------------------
    def Calc_Indicator7(self,date,**params):
        """
        Alpha#7: ((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1* 1))
        """
        t1 = params["t1"]
        t2 = params["t2"]
        t3 = params["t3"]        
        dateBeg= self.CalcDateNDaysBefore(date,t1+t2+t3)
        data = self.dataPanel[:,dateBeg:date,:]
        vol = data[:,:,'amt']
        cp = data[:,:,'c']
        deltaP = cp.diff(t3)
        absDeltaP = numpy.abs(deltaP).iloc[-60:]
        tsRank = absDeltaP.rank(axis=0,pct=True).iloc[-1]
        signDeltaP = numpy.sign(deltaP).iloc[-1]
        v1 = -tsRank*signDeltaP
        tsRank.to_csv('tsRank.csv')
        signDeltaP.to_csv('signDeltaP.csv')
        v1.to_csv('v1.csv')
        print absDeltaP
        #absDeltaP.to_csv('absDeltaP.csv')
        vol.to_csv('vol.csv')
        adv20 = pd.rolling_mean(vol,t2).iloc[-1]
        adv20.to_csv('adv.csv')
        v1[adv20>=vol.iloc[-1]]=-1
        vol.iloc[-1].to_csv('vol.csv')
        v1.to_csv('sig.csv')
        sig = v1
        return sig
        
    
        


        
if __name__ == "__main__":
    indicatorGenerator = IndicatorGenerator("MktData\\MktData_Wind_CICC.db", "20100127",1,0)
    indicatorGenerator.LoadDataIntoTimeSeries("000300","000300",["510300","510050","159903"])
    indicatorGenerator.SetStockUniverse("20151123")
    indicatorGenerator.Calc_Indicator7("20151123",t1=60,t2=20,t3=7)
    