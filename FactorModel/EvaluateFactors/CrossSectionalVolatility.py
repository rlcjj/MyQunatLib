#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/8
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
class CrossSectionalVolatility(object):
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
    def CalcVols(self,begDate,endDate,returnHorizon):
        """"""
        self.trdDate = GetTrdDays.GetTradeDays()
        trdDay = []
        for d in self.trdDate:
            if d>=begDate and d<=endDate:
                trdDay.append(d)
        crossSectionalVols = {}
        for _index in self.constituentIndexCode:
            _vols = []
            for day in trdDay:
                stks = self.objConstituentStocks.GetConstituentStocksAtGivenDate(day,[_index])
                numOfStks = len(stks)
                histRet = []
                for stk in stks:
                    hRet = self.objGetFactorValues.GetHistoricalReturns(stk,day,returnHorizon)
                    histRet.append(hRet)
                vol = numpy.nanstd(histRet)
                _vols.append(vol)
            crossSectionalVols[_index] = _vols
        df = pd.DataFrame(crossSectionalVols,index=trdDay)
        return df
    
