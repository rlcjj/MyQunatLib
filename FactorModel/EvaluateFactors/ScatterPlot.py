#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/26
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
class EvaluateFactorByScatterPlot(object):
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
    def _ExcludeStocksOfGivenIndustry(self,stocks,stockFactorValues,rptType):
        """"""
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["RptType"]!=None:
                if stockFactorValues[stk]["RptType"]==1:
                    refinedStocks.append(stk)
        return refinedStocks
    
    
    #----------------------------------------------------------------------
    def GenerateTradeList(self,cutOffPoint):
        """"""
        re = open("results.csv",'w')
        self.objGetFactorValues.ChooseFactors(["ReportType","EBIT2EV_TTM","ROIC_TTM"],["FloatCap1d","FRet120d"],[])
        for day in self.rebalaceDays:
            stks = self.objConstituentStocks.GetAllStocksExcludedAfterGivenDate(day,self.constituentIndexCode)
            stkFctVals = self._FetchStockFactorValues(day,stks,180)
            stks = self._ExcludeStocksOfSmallCap(stks,stkFctVals,cutOffPoint)
            stks = self._ExcludeStocksOfGivenInsusty(stks,stkFctVals,1)
            re.write(day+',')
            re.write("StockCode")
            ebit2ev=[]
            roic=[]
            ret=[]
            for s in stks:
                ebit2ev.append(stkFctVals[s]["EBIT2EV_TTM"])
                roic.append(stkFctVals[s]["ROIC_TTM"])
                ret.append(stkFctVals[s]["FRet120d"])
            xlabel = "ebit2ev"
            ylabel = "roic"
            title = day+"_future_return_120d"
            path = day+"_future_return_120d.jpeg"
            ScatterPlot.ScatterPlot(ebit2ev,roic,ret,xlabel,ylabel,title,path)
                
                
            for s in stks:
                re.write(','+s)
            re.write("\n")
            re.write(day+',')
            re.write("EBIT2EV")
            for s in stks:
                re.write(','+repr(stkFctVals[s]["EBIT2EV_TTM"])) 
            re.write("\n")
            re.write(day+',')
            re.write("ROIC")
            for s in stks:
                re.write(','+repr(stkFctVals[s]["ROIC_TTM"]))  
            re.write("\n")
            re.write(day+',')
            re.write("120dReturn")
            for s in stks:
                re.write(','+repr(stkFctVals[s]["FRet120d"]))  
            re.write("\n")            
        re.close()