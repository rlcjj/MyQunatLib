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
    def _ExcludeStockStopTrading(self,stocks,stockFactorValues):
        """"""
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["TradeStatus"]!=None:
                if stockFactorValues[stk]["TradeStatus"]==-1 and  stockFactorValues[stk]["Ret1d"]<0.1:
                    refinedStocks.append(stk)
        return refinedStocks         
    
    
    #----------------------------------------------------------------------
    def _ExcludeFinancialReportType(self,stocks,stockFactorValues,rptTypes):
        """"""
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["RptType"]!=None:
                if not stockFactorValues[stk]["RptType"] in rptTypes:
                    #print stockFactorValues[stk]["RptType"],type(stockFactorValues[stk]["RptType"])
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
    def GeneratePortfolios(self,factorName1,factorName2,effectiveDate,quantile,rptTypes,seq):
        """""" 
        self.objGetFactorValues.ChooseFactors([factorName1,factorName2],["Ret1d","TradeStatus"],[])
        portfolios = {}
        self.outputFactorVals = {"date":[],"vals":[]}
        for day in self.rebalaceDays:
            stks = self.objConstituentStocks.GetConstituentStocksAtGivenDate(day,self.constituentIndexCode)
            numOfStks = len(stks)
            numSelected = int(quantile*numOfStks)  
            #print numOfStks,numSelected
            stkFctVals = self._FetchStockFactorValues(day,stks,effectiveDate)
            stks = self._ExcludeFinancialReportType(stks,stkFctVals,rptTypes)
            stkd = self._ExcludeStockStopTrading(stks,stkFctVals)
            port = {}
            _factorDict1 = {}
            _factorDict2 = {}
            _longList = []
            _shortList = []
            sortedStksDict1 = {}
            sortedStksDict2 = {}
            i = 0
            j = 0
            for stk in stks:
                if not numpy.isnan(stkFctVals[stk][factorName1]):
                    _factorDict1[stk]=stkFctVals[stk][factorName1]
                if not numpy.isnan(stkFctVals[stk][factorName2]):
                    _factorDict2[stk]=stkFctVals[stk][factorName2]                
            for stk in sorted(_factorDict1,key=_factorDict1.get,reverse=True):
                sortedStksDict1[stk] = i
                i+=1
            for stk in sorted(_factorDict2,key=_factorDict2.get,reverse=True):
                sortedStksDict2[stk] = j
                j+=1    
            _factorDict = {}
            for stk in sortedStksDict1.keys():
                if stk in sortedStksDict2.keys():
                    _factorDict[stk] = sortedStksDict1[stk]+sortedStksDict2[stk]
                    
            sortedStks = []
            for stk in sorted(_factorDict,key=_factorDict.get):
                sortedStks.append(stk)            
                
            if len(sortedStks)>2*numSelected:
                if seq==1:
                    port["long"] = sortedStks[0:numSelected]
                    port["short"] = sortedStks[-numSelected:]     
                else:
                    port["long"] = sortedStks[-numSelected:]
                    port["short"] = sortedStks[0:numSelected]
            else:
                port["long"] = []
                port["short"] = []
            portfolios[day] = port
            self.outputFactorVals["date"].append(day)
            self.outputFactorVals["vals"].append(_factorDict)
            self.factorName = factorName1+factorName2
        return portfolios
    
    
    #----------------------------------------------------------------------
    def Output(self,path):
        """"""
        if not os.path.exists(path):
            os.makedirs(path)
        f = open("{}FactorValues_{}.csv".format(path,self.factorName),'w')
        for i in xrange(len(self.outputFactorVals["date"])):
            f.write(self.outputFactorVals["date"][i])
            for stk in sorted(self.outputFactorVals["vals"][i],key=self.outputFactorVals["vals"][i].get,reverse=True): 
                f.write(",{}".format(stk))
            f.write("\n")
            for stk in sorted(self.outputFactorVals["vals"][i],key=self.outputFactorVals["vals"][i].get,reverse=True): 
                f.write(",{}".format(self.outputFactorVals["vals"][i][stk]))
            f.write("\n") 
        f.close()

