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
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import FundamentalAnalysis.FactorPortfolio.SelectStksByFctVals as SelectStks
import FundamentalAnalysis.FactorPortfolio.CalcPortRets as CalcPort
import DefineInvestUniverse.GetIndexCompStocks as IndexCompStks


########################################################################
class GetFactorPortReturn(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finRptDataDbAddr,mktDataDbAddr,indexCompStksDataDbAddr):
        """Constructor"""
        localRawDataDBPath = GetPath.GetLocalDatabasePath()["RawEquity"]
        localProcDataDBPath = GetPath.GetLocalDatabasePath()["ProcEquity"]
        self._trdDays = GetTrdDay.GetTradeDays()
        
        self.selectStks = SelectStks.SelectStksByFctVals(finRptDataDbAddr,mktDataDbAddr)
        self.calcPortRet = CalcPort.CalcPortRets(mktDataDbAddr)
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
    def FactorPortReturn(self,indexCode,factorName,percentile):
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
        
        
        


if __name__ == "__main__":
    import FundamentalAnalysis.FdmtFactorVal.FactorAlgos.E2P_TTM as E2P_TTM
    dbPath1 = "test.db"
    dbPath2 = "MktData\\MktData_Wind_CICC.db"   
    dbPath3 = "MktGenInfo\\IndexComp_Wind_CICC.db"
    getFctPortRet = GetFactorPortReturn(dbPath1,dbPath2,dbPath3)
    getFctPortRet.SetRebalanceDate("20140101","20151118",20)
    getFctPortRet.FactorPortReturn("000300",E2P_TTM,0.2)
        
    
    