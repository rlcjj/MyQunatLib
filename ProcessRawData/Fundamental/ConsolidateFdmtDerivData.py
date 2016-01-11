#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/15
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import DefineInvestUniverse.GetIndexCompStocks as CompStks
import ProcessRawData.Fundamental.GetFdmtDerivData as FdmtData

########################################################################
class ConsolidateData(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logOutputHandler):
        """Constructor"""
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        for lh in logOutputHandler:
            self.logger.addHandler(lh)  
        
        self.locDbPath = GetPath.GetLocalDatabasePath()
        
    #----------------------------------------------------------------------
    def SetStockUniverse(self,dbAddress,indexCode):
        """"""
        self.indexCode = indexCode
        self.compStks = CompStks.GetIndexCompStocks(dbAddress)
        self.indexAdjDate = self.compStks. GetIndexAdjustDate(indexCode)        


    #----------------------------------------------------------------------
    def AppendFinRptItems(self,items):
        """"""
        self.items1 = []
        for item in items:
            self.items1.append(item)
            
    #----------------------------------------------------------------------
    def AppendFcstItems(self,items):
        """"""
        self.items2 = []
        for item in items:
            self.items2.append(item)        
        
    
    #----------------------------------------------------------------------
    def CreateDatabase(self,indicDbName):
        """"""
        indicDbAdrr = self.locDbPath["ProcEquity"]+indicDbName
        self.indicConn = lite.connect(indicDbAdrr)
        cur = self.indicConn.cursor()
        cur.execute("DROP TABLE IF EXISTS FinRptDerivData")
        cur.execute("DROP TABLE IF EXISTS ForecastData")
        cur.execute("PRAGMA synchronous = OFF")
        sqlStr = ""
        for item in self.items1:
            itemName = item.__name__.split('.')[-1]
            sqlStr+=","+itemName+" FLOAT"
        cur.execute("""
                    CREATE TABLE FinRptDerivData(StkCode TEXT,
                                                 AcctPeriod TEXT,
                                                 DeclareDate TEXT
                                                 {})
                    """.format(sqlStr))
        sqlStr = ""
        for item in self.items2:
            itemName = item.__name__.split('.')[-1]
            sqlStr+=","+itemName+" FLOAT"
        cur.execute("""
                    CREATE TABLE ForecastData(StkCode TEXT,
                                              AcctPeriod TEXT,
                                              DeclareDate TEXT
                                              {})
                    """.format(sqlStr))        
        
    
    #----------------------------------------------------------------------
    def GenerateData(self):
        """"""
        finRptDbPath = self.locDbPath["RawEquity"]+"FinRptData\\FinRptData_Wind_CICC.db"
        mktDataDbPath = self.locDbPath["RawEquity"]+"MktData\\MktData_Wind_CICC.db"
        fdmtData = FdmtData.GetFdmtDerivData(finRptDbPath,mktDataDbPath)
        allStks = self.compStks.GetAllStocks('000300')
        
        cur = self.indicConn.cursor()
        lenOfItems = len(self.items1)
        insertSql = "?,?,?"+lenOfItems*",?"
        for stk in allStks:
            print stk
            date = self.compStks.GetIncludeAndExcludeDate(stk,'000300')
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = fdmtData.GetFinDataDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                acctPeriod = ""
                val = []
                #for item in self.items:
                itemVals = fdmtData.CalcFinRptDerivData(dt,300,stk,self.items1)
                if itemVals!=None:
                    for itemVal in itemVals:
                        if itemVal == None:
                            acctPeriod = None
                            _val = None
                        else:
                            acctPeriod = itemVal[0]
                            _val = itemVal[1]
                        val.append(_val)
                    row = [stk,acctPeriod,dt]
                    for v in val:
                        row.append(v)
                    cur.execute("INSERT INTO FinRptDerivData VALUES ({})".format(insertSql),tuple(row))
        self.indicConn.commit()
        
        cur = self.indicConn.cursor()
        lenOfItems = len(self.items2)
        insertSql = "?,?,?"+lenOfItems*",?"
        for stk in allStks:
            print stk
            date = self.compStks.GetIncludeAndExcludeDate(stk,'000300')
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = fdmtData.GetForecastDataDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                acctPeriod = ""
                val = []
                #for item in self.items:
                itemVals = fdmtData.CalcForecastDerivData(dt,300,stk,self.items2)
                if itemVals!=None:
                    for itemVal in itemVals:
                        if itemVal == None:
                            acctPeriod = None
                            _val = None
                        else:
                            acctPeriod = itemVal[0]
                            _val = itemVal[1]
                        val.append(_val)
                    row = [stk,acctPeriod,dt]
                    for v in val:
                        row.append(v)
                    cur.execute("INSERT INTO ForecastData VALUES ({})".format(insertSql),tuple(row))
        self.indicConn.commit()        
                    
            
            

    