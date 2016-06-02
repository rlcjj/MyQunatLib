#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/1
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
class HedgedPortfolio(object):
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
    def _ExcludeFinancialReportType(self,stocks,stockFactorValues,rptTypes):
        """"""
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["RptType"]!=None:
                if not stockFactorValues[stk]["RptType"] in rptTypes:
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
    def _Trim(self,lst,percentile):
        """"""
        _lst = []
        p1 = numpy.nanpercentile(lst,percentile,interpolation='lower')
        p2 = numpy.nanpercentile(lst,100-percentile,interpolation='higher')
        for item in lst:
            if item>p1:
                _lst.append(numpy.nan)
            elif item<=p1 and item>p2:
                _lst.append(item)
            elif item<=p2:
                _lst.append(numpy.nan)
            else:
                _lst.append(item)
        return _lst    
    
    #----------------------------------------------------------------------
    def GeneratePortfolios(self,factors,effectiveDate,quantile,rptTypes,seq):
        """""" 
        self.objGetFactorValues.ChooseFactors([factors,"ROIC_TTM","ChangeInGrossMargin_TTM"],["logsize","FRet20d"],[])
        portfolios = {}
        for day in self.rebalaceDays:
            stks = self.objConstituentStocks.GetConstituentStocksAtGivenDate(day,self.constituentIndexCode)
            numOfStks = len(stks)
            numSelected = int(quantile*numOfStks)  
            #print numOfStks,numSelected
            stkFctVals = self._FetchStockFactorValues(day,stks,effectiveDate)
            stks = self._ExcludeFinancialReportType(stks,stkFctVals,rptTypes)         
            port = {}
            _factorDict = {}
            _longList = []
            _shortList = []
            sortedStks = []
            for stk in stks:
                if stkFctVals[stk]!=numpy.nan:
                    _factorDict[stk]=stkFctVals[stk][factors]
            for stk in sorted(_factorDict,key=_factorDict.get,reverse=True):
                sortedStks.append(stk)
            if seq==1:
                port["long"] = sortedStks[0:numSelected]
                port["short"] = sortedStks[-numSelected:]     
            else:
                port["long"] = sortedStks[-numSelected:]
                port["short"] = sortedStks[0:numSelected] 
            portfolios[day] = port
        return portfolios
            
            
            
    
    