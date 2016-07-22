#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/7
"""


import os,numpy
from datetime import datetime,timedelta
import sqlite3 as lite
import pandas as pd
import Tools.GetLocalDatabasePath as GetPath
import InvestmentUniverse.GetIndexConstituentStocks as GetIndexConstituentStocks
import Tools.GetTradeDays as GetTrdDays
import Tools.ScatterPlot as ScatterPlot
import Tools.Draw as Draw
import FactorModel.GetFactorValues as modGetFactorValues
import matplotlib.pyplot as pl
import Configs.RootPath as Root
RootPath = Root.RootPath


########################################################################
class IC(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logger):
        """Constructor"""
        self.objGetFactorValues = modGetFactorValues.GetFactorValues()
        self.logger = logger
        self.logger.info("<{}>-Initiate object".format(__name__.split('.')[-1]))
    
    
    #----------------------------------------------------------------------
    def LoadFactorDatabase(self,dbNameFactor,factorTypes):
        """"""
        self.logger.info("<{}>-Load local database".format(__name__.split('.')[-1]))
        self.objGetFactorValues.LoadFactorTablesIntoMemory(dbNameFactor,factorTypes)
        
        
    #----------------------------------------------------------------------
    def SetStockUniverse(self,dbPathIndexConstituents,constituentIndexCode):
        """"""
        self.logger.info("<{}>-Set stock universe: {}".format(__name__.split('.')[-1],constituentIndexCode))
        self.constituentIndexCode = constituentIndexCode
        self.objConstituentStocks = GetIndexConstituentStocks.GetIndexConstituentStocks(dbPathIndexConstituents,self.logger)
        
    
    #----------------------------------------------------------------------
    def SetRebalanceDate(self,begDate,endDate,freq,day):
        """"""
        self.logger.info("<{}>-Set rebalance frequency: {}".format(__name__.split('.')[-1],freq))
        self.objTradeDays = GetTrdDays.TradeDays()
        self.objTradeDays.GetTradeDays()
        self.rebalaceDays = self.objTradeDays.GetRebalanceDays(begDate,endDate,freq,day)
            
    #----------------------------------------------------------------------
    def _FetchStockFactorValues(self,date,stks,effectiveTime):
        """"""
        self.logger.info("<{}>-Fetch factor values".format(__name__.split('.')[-1]))
        stockFactorValues = {}
        for stk in stks:
            factVal = self.objGetFactorValues.GetFactorValues(stk,date,effectiveTime)
            stockFactorValues[stk]=factVal
        return stockFactorValues
    
    
    #----------------------------------------------------------------------
    def _ExcludeStockStopTrading(self,stocks,stockFactorValues):
        """"""
        self.logger.info("<{}>-Exclude stop trading stocks".format(__name__.split('.')[-1]))
        refinedStocks = []
        for stk in stocks:
            if stockFactorValues[stk]["TradeStatus"]!=None:
                if ((stockFactorValues[stk]["TradeStatus"]==-1 or stockFactorValues[stk]["TradeStatus"]==2 
                     or stockFactorValues[stk]["TradeStatus"]==3 or stockFactorValues[stk]["TradeStatus"]==4)
                    and stockFactorValues[stk]["Ret1d"]<0.0998 and stockFactorValues[stk]["Ret1d"]>-0.0998):
                    refinedStocks.append(stk)
        return refinedStocks         
    
    
    #----------------------------------------------------------------------
    def _ExcludeFinancialReportType(self,stocks,stockFactorValues,rptTypes):
        """"""
        self.logger.info("<{}>-Exclude specified industries".format(__name__.split('.')[-1]))
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
        self.logger.info("<{}>-Winsorize data".format(__name__.split('.')[-1]))
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
        self.logger.info("<{}>-Trim data".format(__name__.split('.')[-1]))
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
    def CalcICs(self,factorBanks,factorName,returnHorizon,effectiveDate,rptTypes,outlier,percentile):
        """""" 
        self.logger.info("<{}>-Generate portfolios and calculate ICs".format(__name__.split('.')[-1]))
        self.objGetFactorValues.ChooseFactors(factorBanks[0],factorBanks[1],factorBanks[2])
        IC = {}
        for day in self.rebalaceDays:
            stks = self.objConstituentStocks.GetConstituentStocksAtGivenDate(day,self.constituentIndexCode)
            numOfStks = len(stks)
            #print numOfStks,numSelected
            stkFctVals = self._FetchStockFactorValues(day,stks,effectiveDate)
            stks = self._ExcludeFinancialReportType(stks,stkFctVals,rptTypes)
            stks = self._ExcludeStockStopTrading(stks,stkFctVals)

            self.logger.info("<{}>-Calculating ICs-{}".format(__name__.split('.')[-1],day))
            _factorValList = []
            corr = []
            for stk in stks:
                _factorValList.append(stkFctVals[stk][factorName])
            if outlier==1:
                _factorValList = self._Winsorzie(_factorValList, percentile)
            if outlier==2:
                _factorValList = self._Trim(_factorValList, percentile)  
            
            for days in returnHorizon:
                _futureRetList = []
                for stk in stks:
                    fRet = self.objGetFactorValues.GetFutureReturns(stk,day,days)
                    _futureRetList.append(fRet)
                mat = pd.DataFrame([_factorValList,_futureRetList]).transpose()
                #print len(_futureRetList)
                #print len(_factorValList)
                _corr = mat.corr(method="spearman").values[0,1]
                corr.append(_corr)
            IC[day] = corr
        return IC
    
    
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
        
        

            
            
            
    
    