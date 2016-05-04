#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 根据单一因子获取对冲组合
  Created: 2015/12/17
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
import time
from datetime import datetime,timedelta

import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import Tools.Draw as Draw
import FactorModel.EvaluateFactorByHedgedPortfolioReturn._CalculatePortfolioReturn as CalcPortRet


########################################################################
class GetHedgedPortfolioBySingleFactor(object):
    """
    根据单一因子获取对冲组合
    """

    #----------------------------------------------------------------------
    def __init__(self,dbPathFactorValues,dbPathMarketData,begDate,endDate,logger=None):
        """Constructor"""
        if logger == None:
            self.logger = logging.logger()
        else:
            self.logger = logger
        
        dbPathRawData = GetPath.GetLocalDatabasePath()["RawEquity"]
        dbPathProcessedData = GetPath.GetLocalDatabasePath()["ProcEquity"]   
        
        #价格数据录入内存数据库
        self.connDbMkt = lite.connect(":memory:")
        self.connDbMkt.text_factory = str
        cur = self.connDbMkt.cursor()
        self.logger.info("<{}>-Load local database into in-memory database".format(__name__.split('.')[-1]))
        _dbPathMarketData = dbPathRawData + dbPathMarketData 
        cur.execute("ATTACH '{}' AS MktData".format(_dbPathMarketData))
        cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,LC,TC FROM MktData.AStockData WHERE Date>='{}'".format(begDate))
        cur.execute("CREATE INDEX mId ON MktData (Date,StkCode)")
        self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))         
        
        #连接因子数据库
        dbFactorValue = dbPathProcessedData + dbPathFactorValues
        self.connDbFV = lite.connect(dbFactorValue)
        self.connDbFV.text_factory = str
        
        #获取交易日
        self._trdDays = GetTrdDay.GetTradeDays()        
        self.revalueDays = []
        self.trdDays = []
        cur = self.connDbFV.cursor()
        cur.execute("SELECT DISTINCT Date FROM FactorValues ORDER BY Date ASC")
        rows = cur.fetchall()
        for row in rows:
            self.revalueDays.append(row[0])
        for d in self._trdDays:
            if d>=self.revalueDays[0] and d<=endDate and d>=begDate:
                self.trdDays.append(d)
        
        #Initialte CalculatePortfolioReturn   
        self.objCalcPortReturn = CalcPortRet.CalculatePortfolioReturn(dbPathMarketData,self.connDbMkt,self.logger)
    
    
    #----------------------------------------------------------------------
    def GetFactorNames(self):
        """
        获取数据库中所有的因子名称
        """
        cur = self.connDbFV.cursor()
        cur.execute("PRAGMA table_info(FactorValues)")
        cols = cur.fetchall()
        factors = []
        for col in cols[8:]:
            factors.append(col[1])
        return factors
        
    
    #----------------------------------------------------------------------
    def SortStocksByFactorValue(self,date,InHS300,factorName,tableName,order,excludeIndus,):
        """
        根据基本面因子值选择股票
        """
        cur = self.connDbFV.cursor()
        cur.execute("""
                    SELECT StkCode,{} 
                    FROM {} 
                    WHERE HS300Constituent IN ({}) AND Date='{}' AND ReportType NOT IN ({}) 
                    ORDER BY {} {}
                    """.format(factorName,tableName,InHS300,date,excludeIndus,factorName,order))
        rows = cur.fetchall()
        totalStks = []
        sortedStks = []
        for row in rows:
            totalStks.append(row[0])
            if row[1]!=None:
                sortedStks.append(row[0])
        return totalStks,sortedStks
    
    
    #----------------------------------------------------------------------
    def CalculateHedgedPortfolioReturn(self,factorName,tableName,InHS300,excludeIndus,percentile,order,plotPath,plot=1):
        """
        计算对冲组合的累积收益
        """
        date = []
        hedgedPortReturn = []
        benchmarkReturn = []
        longPort = []
        shortPort = []
        benchMarkPort = []
        for dt in self.trdDays:
            if dt in self.revalueDays:
                tm1 = time.time()
                stks = self.SortStocksByFactorValue(dt,InHS300,factorName,tableName,order,excludeIndus)
                tm2 = time.time()
                numStk = int(len(stks[0])*percentile)
                #print dt,len(stks[0]),len(stks[1])
                if len(stks[1])>=0.4*len(stks[0]):
                    longPort = stks[1][0:numStk]
                    shortPort = stks[1][-numStk:]
                benchMarkPort = stks[0]
            tm3 = time.time()
            longRet = self.objCalcPortReturn.Calc(dt,longPort)
            shortRet = self.objCalcPortReturn.Calc(dt,shortPort)
            benchmarkRet = self.objCalcPortReturn.Calc(dt,benchMarkPort)
            tm4 = time.time()
            #print tm2-tm1,tm4-tm3
            ret = longRet-shortRet
            date.append(dt)
            hedgedPortReturn.append(ret)
            benchmarkReturn.append(benchmarkRet)
            #print dt,ret     
        if InHS300 == "1":
            univer = "(Stock universe: hs300)"
        elif InHS300 == "0":
            univer = "(Stock universe: zz500)"
        elif InHS300 == "0,1":
            univer = "(Stock universe: zz800)"
            
        if plot == 1:
            Draw.DrawCumulativeReturnCurve(date,hedgedPortReturn,factorName+'_'+univer,plotPath+"\\"+factorName+"FactorPortReturn.jpeg",benchmarkReturn)
            self.logger.info("<{}>-Plot hedged return of factor {}".format(__name__.split('.')[-1],factorName)) 
                
        
if __name__ == "__main__":
    dbPath1 = "Factor_399906_20D.db"
    dbPath2 = "MktData\\MktData_Wind_CICC.db"   
    portReturns = GetFactorPortReturns(dbPath1,dbPath2,"20151231")
    portReturns.CalcLongMinusBenchMarkReturns("B2P","FactorVals","0","2",0.2,"DESC","Plot_ZZ500")
    
