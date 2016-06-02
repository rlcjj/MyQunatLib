#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/24
"""

import os,numpy
from datetime import datetime,timedelta
import sqlite3 as lite
import Tools.GetLocalDatabasePath as GetPath
import InvestmentUniverse.GetIndexConstituentStocks as GetIndexConstituentStocks
import Tools.GetTradeDays as GetTrdDays
import Tools.ScatterPlot as ScatterPlot
import FactorModel.GetFactorValues as modGetFactorValues
import matplotlib.pyplot as pl
import Configs.RootPath as Root
RootPath = Root.RootPath


########################################################################
class MagicFormula(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logger):
        """Constructor"""
        self.objGetFactorValues = modGetFactorValues.GetFactorValues()
        self.logger = logger
    
    
    #----------------------------------------------------------------------
    def LoadFactorDatabase(self,dbNameFactor,factorTypes):
        """"""
        self.objGetFactorValues.LoadFactorTablesIntoMemory(dbNameFactor,factorTypes)
        
        
    #----------------------------------------------------------------------
    def SetStockUniverse(self,dbPathIndexConstituents,constituentIndexCode):
        """"""
        self.constituentIndexCode = constituentIndexCode
        self.objConstituentStocks = GetIndexConstituentStocks.GetIndexConstituentStocks(dbPathIndexConstituents,self.logger)
        
    
    #----------------------------------------------------------------------
    def SetRebalanceDate(self,begDate,endDate,freq,day):
        """"""
        self.objTradeDays = GetTrdDays.TradeDays()
        self.objTradeDays.GetTradeDays()
        self.rebalaceDays = self.objTradeDays.GetRebalanceDays(begDate,endDate,freq,day)
            
    #----------------------------------------------------------------------
    def _FetchStockFactorValues(self,date,stks,effectiveTime):
        """"""
        stockFactorValues = {}
        for stk in stks:
            factVal = self.objGetFactorValues.GetFactorValues(stk,date,effectiveTime)
            stockFactorValues[stk]=factVal
        return stockFactorValues
        
        
    #----------------------------------------------------------------------
    def _ExcludeStocksOfSmallCap(self,stocks,stockFactorValues,cutOffPoint):
        """"""
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["FloatCap1d"]>=cutOffPoint:
                refinedStocks.append(stk)
        return refinedStocks
    
    
    #----------------------------------------------------------------------
    def _ExcludeStocksOfGivenInsusty(self,stocks,stockFactorValues,rptType):
        """"""
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["RptType"]!=None:
                if stockFactorValues[stk]["RptType"]==1:
                    refinedStocks.append(stk)
        return refinedStocks
    

    #----------------------------------------------------------------------
    def _Winsorzie(self,lst,percentile):
        """"""
        _lst = []
        p1 = numpy.nanpercentile(lst,percentile,interpolation='lower')
        p2 = numpy.nanpercentile(lst,100-percentile,interpolation='higher')
        for item in lst:
            if item>p1:
                _lst.append(p1)
            elif item<=p1 and item>p2:
                _lst.append(item)
            elif item<=p2:
                _lst.append(p2)
            else:
                _lst.append(item)
        return _lst
                    
        
    
    #----------------------------------------------------------------------
    def GenerateTradeList(self,cutOffPoint):
        """"""
        re = open("results.csv",'w')
        self.objGetFactorValues.ChooseFactors(["EBIT2EV_TTM","ROIC_TTM","ChangeInGrossMargin_TTM"],["logsize","FRet20d"],[])
        fdmtFactor1 = "EBIT2EV_TTM"
        fdmtFactor2 = "FRet20d"  
        predictRet = "FRet20d"
        for day in self.rebalaceDays:
            stks = self.objConstituentStocks.GetAllStocksExcludedAfterGivenDate(day,self.constituentIndexCode)
            stkFctVals = self._FetchStockFactorValues(day,stks,180)
            #stks = self._ExcludeStocksOfSmallCap(stks,stkFctVals,cutOffPoint)
            stks = self._ExcludeStocksOfGivenInsusty(stks,stkFctVals,1)
            re.write(day+',')
            re.write("StockCode")
            ebit2ev=[]
            roic=[]
            ret=[]
            for s in stks:
                ebit2ev.append(stkFctVals[s][fdmtFactor1])
                roic.append(stkFctVals[s][fdmtFactor2])
                ret.append(stkFctVals[s][predictRet])
            ebit2ev = self._Winsorzie(ebit2ev,99.5)
            roic = self._Winsorzie(roic,99.5)            
                
                
                
            xlabel = fdmtFactor1
            ylabel = fdmtFactor2
            title = day+"_future_"+predictRet+".jpeg"
            path = day+"_future_"+predictRet+".jpeg"
            c=ScatterPlot.ScatterPlot(ebit2ev,roic,ret,xlabel,ylabel,title,path)
                
                
            for s in stks:
                re.write(','+s)
            re.write("\n")
            re.write(day+',')
            re.write(fdmtFactor1)
            for s in stks:
                re.write(','+repr(stkFctVals[s][fdmtFactor1])) 
            re.write("\n")
            re.write(day+',')
            re.write(fdmtFactor2)
            for s in stks:
                re.write(','+repr(stkFctVals[s][fdmtFactor2]))  
            re.write("\n")
            re.write(day+',')
            re.write(predictRet)
            for s in stks:
                re.write(','+repr(stkFctVals[s][predictRet]))  
            re.write("\n")    
            re.write(day+',')
            re.write("Colour")
            for i in range(len(stks)):
                re.write(','+c[i])  
            re.write("\n")             
        re.close()
        
        

            

        
        
    
    