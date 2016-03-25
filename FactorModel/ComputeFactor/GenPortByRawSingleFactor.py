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
    def SortStocksByFactor(self,date,InHS300,factorName,tableName,order,excludeIndus,):
        """"""
        cur = self.conn2.cursor()
        cur.execute("""SELECT StkCode,{} 
                       FROM {} 
                       WHERE InHS300 IN ({}) AND Date='{}' AND ReportType NOT IN ({}) 
                       ORDER BY {} {}""".format(factorName,tableName,InHS300,date,excludeIndus,factorName,order))
        rows = cur.fetchall()
        totalStks = []
        sortedStks = []
        for row in rows:
            totalStks.append(row[0])
            if row[1]!=None:
                sortedStks.append(row[0])
        return totalStks,sortedStks
    
    #----------------------------------------------------------------------
    def CalcLongMinusBenchMarkReturns(self,factorName,tableName,InHS300,excludeIndus,percentile,order,plotPath,plot=1):
        """"""
        dates = []
        relativeReturns = []
        benchMarkReturns = []
        longPort = []
        shortPort = []
        #benchMarkPort = []
        for dt in self.trdDays:
            if dt in self.revalueDays:
                stks = self.SortStocksByFactor(dt,InHS300,factorName,tableName,order,excludeIndus)
                numStk = int(len(stks[0])*percentile)
                print dt,len(stks[0]),len(stks[1])
                if len(stks[1])>=0.4*len(stks[0]):
                    longPort = stks[1][0:numStk]
                    shortPort = stks[1][-numStk:]
                benchMarkPort = stks[0]
            longRet = self.calcPortRet.Calc(dt,longPort)
            shortRet = self.calcPortRet.Calc(dt,shortPort)
            benchmarkRet = self.calcPortRet.Calc(dt,benchMarkPort)
            ret = longRet-shortRet
            dates.append(dt)
            relativeReturns.append(ret)
            benchMarkReturns.append(benchmarkRet)
            #print dt,ret     
        if InHS300 == "1":
            univer = "(Stock universe: hs300)"
        elif InHS300 == "0":
            univer = "(Stock universe: zz500)"
        elif InHS300 == "0,1":
            univer = "(Stock universe: zz800)"
            
        if plot == 1:
            Draw.DrawCumulativeReturnCurve(dates,relativeReturns,factorName+'_'+univer,plotPath+"\\"+factorName+"FactorPortReturn.jpeg",benchMarkReturns)
            
                
        
if __name__ == "__main__":
    dbPath1 = "Factor_399906_20D.db"
    dbPath2 = "MktData\\MktData_Wind_CICC.db"   
    portReturns = GetFactorPortReturns(dbPath1,dbPath2,"20151231")
    portReturns.CalcLongMinusBenchMarkReturns("B2P","FactorVals","0","2",0.2,"DESC","Plot_ZZ500")
    
