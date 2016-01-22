#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import Tools.Draw as Draw
import FactorAnalysis.TestRawFactorPortfolio.SelectStksByFctVals as SelectStks
import FactorAnalysis.TestRawFactorPortfolio.CalcPortRets as CalcPort
import DefineInvestUniverse.GetIndexCompStocks as IndexCompStks


########################################################################
class GetFactorPortReturns(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finRptDataDbAddr,mktDataDbAddr,indexCompStksDataDbAddr):
        """Constructor"""
        localRawDataDBPath = GetPath.GetLocalDatabasePath()["RawEquity"]
        localProcDataDBPath = GetPath.GetLocalDatabasePath()["ProcEquity"]
        self._trdDays = GetTrdDay.GetTradeDays()
        
        self.selectStks = SelectStks.SelectStksByFctVals(finRptDataDbAddr,mktDataDbAddr)
        self.calcPortRet = CalcPort.CalcPortRets(mktDataDbAddr,self.selectStks.getFctVal.conn)
        self.indexCompStks = IndexCompStks.GetIndexCompStocks(indexCompStksDataDbAddr)
        
        
    #----------------------------------------------------------------------
    def SetRebalanceDate(self,begDate,endDate,holdingPeriod):
        """"""
        self.trdDays = []
        self.ifRebalance = {}
        k = 0
        for dt in self._trdDays:
            if dt>=begDate and dt<=endDate:
                self.trdDays.append(dt)
                k+=1
                if k % holdingPeriod == 1:
                    self.ifRebalance[dt]=1
                else:
                    self.ifRebalance[dt]=0
        
    #----------------------------------------------------------------------
    def CalcLongShortReturns(self,indexCode,factorName,percentile):
        """"""
        rets = []
        for dt in self.trdDays:
            if self.ifRebalance[dt]==1:
                stkUniver = self.indexCompStks.GetStocks(dt,indexCode)
                ports = self.selectStks.Select(dt,stkUniver,factorName,percentile)
                longPort = ports[0]
                shortPort = ports[1]
            longRet = self.calcPortRet.Calc(dt,longPort)
            shortRet = self.calcPortRet.Calc(dt,shortPort)
            ret = longRet-shortRet
            rets.append(ret)
            print dt,ret 
    
    #----------------------------------------------------------------------
    def CalcLongMinusBenchMarkReturns(self,indexCode,factor,percentile,seq,save=1,plot=1):
        """"""
        rebalanceDate = []
        factorVals = []
        longPorts = []
        dates = []
        relativeReturns = []
        for dt in self.trdDays:
            if self.ifRebalance[dt]==1:
                rebalanceDate.append(dt)
                stkUniver = self.indexCompStks.GetStocks(dt,indexCode)
                ports = self.selectStks.Select(dt,stkUniver,[factor],percentile,seq)
                factorVal = ports[1]
                factorVals.append(factorVal)
                longPort = ports[0]["long"]
                longPorts.append(longPort)
                shortPort = stkUniver
            longRet = self.calcPortRet.Calc(dt,longPort)
            shortRet = self.calcPortRet.Calc(dt,shortPort)
            ret = longRet-shortRet
            dates.append(dt)
            relativeReturns.append(ret)
            print dt,ret     
        
        if save==1:
            factorName = factor.__name__.split('.')[-1]
            if not os.path.exists("FactorValues\\"+factorName):
                os.makedirs("FactorValues\\"+factorName)

            factValFile = open("FactorValues\\"+factorName+"\\FactorValues.csv",'w')
            stkList = []
            factValFile.write(factorName)
            for i in xrange(len(rebalanceDate)):
                for stk in factorVals[i].keys():
                    if not stk in stkList:
                        stkList.append(stk)    
                        factValFile.write(','+stk)
            factValFile.write("\n")
            for i in xrange(len(rebalanceDate)):
                factValFile.write(rebalanceDate[i])
                for stk in stkList:
                    if factorVals[i].has_key(stk):
                        factValFile.write(','+repr(factorVals[i][stk]))
                    else:
                        factValFile.write(', None')
                factValFile.write("\n")
            factValFile.close()
            
            longPortFile = open("FactorValues\\"+factorName+"\\LongPorts.csv","w")
            for i in xrange(len(rebalanceDate)):
                longPortFile.write(rebalanceDate[i])
                for stk in longPorts[i]:
                    longPortFile.write(','+stk)
                longPortFile.write("\n")
            longPortFile.close()
            
        if plot == 1:
            Draw.DrawCumulativeReturnCurve(dates,relativeReturns,factorName,"FactorValues\\"+factorName+"\\FactorPortReturn.jpeg")
            
                
                
            

