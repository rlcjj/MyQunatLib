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
import FactorAlgos.SelectStocksByRawFactorValues as SelectStks
import FactorAlgos.CalcPortRets as CalcPort
import DefineInvestUniverse.GetIndexCompStocks as IndexCompStks


########################################################################
class GetFactorPortReturns(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,factorDatabase,mktDataDbAddr,endDate):
        """Constructor"""
        localRawDataDBPath = GetPath.GetLocalDatabasePath()["RawEquity"]
        localProcDataDBPath = GetPath.GetLocalDatabasePath()["ProcEquity"]
        _mktDataDbAddr = localRawDataDBPath + mktDataDbAddr
        self._trdDays = GetTrdDay.GetTradeDays()
        
        _factorDbAddr = localProcDataDBPath + factorDatabase
        self.conn2 = lite.connect(_factorDbAddr)
        self.conn2.text_factory = str
        
        self.revalueDays = []
        self.trdDays = []
        cur = self.conn2.cursor()
        cur.execute("SELECT DISTINCT Date FROM FactorVals ORDER BY Date ASC")
        rows = cur.fetchall()
        for row in rows:
            self.revalueDays.append(row[0])
        for d in self._trdDays:
            if d>=self.revalueDays[0] and d<=endDate:
                self.trdDays.append(d)
        
        _mktDataDbAddr = localRawDataDBPath + mktDataDbAddr        
        self.conn1 = lite.connect(":memory:")
        self.conn1.text_factory = str
        cur = self.conn1.cursor()
        print "Load local database into in-memory database" 
        cur.execute("ATTACH '{}' AS MktData".format(_mktDataDbAddr))
        cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,LC,TC FROM MktData.A_Share_Data WHERE Date>='{}'".format(self.revalueDays[0]))
        print "Finished"
        cur.execute("CREATE INDEX mId ON MktData (Date,StkCode)")
        print "Finished"           
                
        self.calcPortRet = CalcPort.CalcPortRets(mktDataDbAddr,self.conn1)
        
    
    #----------------------------------------------------------------------
    def SortStocksByFactor(self,date,stkUniverse,factorName,ifZScore,order,execludeIndus):
        """"""
        cur = self.conn2.cursor()
        cur.execute("SELECT StkCode,{} FROM {} WHERE Date='{}' AND IndusCode NOT IN ({}) ORDER BY {} {}".format(factorName,ifZScore,date,execludeIndus,factorName,order))
        rows = cur.fetchall()
        totalStks = []
        sortedStks = []
        for row in rows:
            totalStks.append(row[0])
            if row[1]!=None:
                sortedStks.append(row[0])
        return totalStks,sortedStks
    
    #----------------------------------------------------------------------
    def CalcLongMinusBenchMarkReturns(self,factorName,ifZScore,excludeIndus,percentile,order,plot=1):
        """"""
        dates = []
        relativeReturns = []
        longPort = []
        shortPort = []
        for dt in self.trdDays:
            if dt in self.revalueDays:
                stks = self.SortStocksByFactor(dt, factorName,ifZScore,order,excludeIndus)
                numStk = int(len(stks[0])*percentile)
                print dt,len(stks[0]),len(stks[1])
                if len(stks[1])>=0.6*len(stks[0]):
                    longPort = stks[1][0:numStk]
                    shortPort = stks[1][-numStk:]
            longRet = self.calcPortRet.Calc(dt,longPort)
            shortRet = self.calcPortRet.Calc(dt,shortPort)
            ret = longRet-shortRet
            dates.append(dt)
            relativeReturns.append(ret)
            #print dt,ret     
        
            
        if plot == 1:
            Draw.DrawCumulativeReturnCurve(dates,relativeReturns,factorName,factorName+"FactorPortReturn.jpeg")
            
                
        
if __name__ == "__main__":
    dbPath1 = "Factor_399906_20D.db"
    dbPath2 = "MktData\\MktData_Wind_CICC.db"   
    portReturns = GetFactorPortReturns(dbPath1,dbPath2,"20151231")
    portReturns.CalcLongMinusBenchMarkReturns("CapitalEmployed2EV","FactorVals","801780,801190,801790",0.2,"DESC")
    
