#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/15
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import DefineInvestUniverse.GetIndexCompStocks as CompStks
import ProcessRawData.Fundamental.CalcFinRptDerivData as CalcFinDeriv

########################################################################
class GenFinRptDerivData(object):
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
    def DefineStockUniverse(self,dbAddress,indexCode):
        """"""
        self.indexCode = indexCode
        self.compStks = CompStks.GetIndexCompStocks(dbAddress)
        self.indexAdjDate = self.compStks. GetIndexAdjustDate(indexCode)        


    #----------------------------------------------------------------------
    def AppendIndicators(self,*indicators):
        """"""
        self.indicators = []
        for indic in indicators:
            self.indicators.append(indic)
    
    #----------------------------------------------------------------------
    def CreateIndicatorDatabase(self,indicDbName):
        """"""
        indicDbAdrr = self.locDbPath["ProcEquity"]+indicDbName
        self.indicConn = lite.connect(indicDbAdrr)
        cur = self.indicConn.cursor()
        cur.execute("DROP TABLE IF EXISTS FinRptDerivData")
        cur.execute("PRAGMA synchronous = OFF")
        sqlStr = ""
        for indic in self.indicators:
            indicName = indic.__name__.split('.')[-1]
            sqlStr+=","+indicName+" FLOAT"
        cur.execute("""
                    CREATE TABLE FinRptDerivData(StkCode TEXT,
                                                 AcctPeriod TEXT,
                                                 DeclareDate TEXT
                                                 {})
                    """.format(sqlStr))
        
    
    #----------------------------------------------------------------------
    def GenDerivData(self):
        """"""
        finRptDbPath = self.locDbPath["RawEquity"]+"FinRptData\\FinRptData_Wind_CICC.db"
        mktDataDbPath = self.locDbPath["RawEquity"]+"MktData\\MktData_Wind_CICC.db"
        calcFinDeriv = CalcFinDeriv.CalcFinRptDerivData(finRptDbPath,mktDataDbPath)
        allStks = self.compStks.GetAllStocks('000300')
        
        cur = self.indicConn.cursor()
        lenOfIndicat = len(self.indicators)
        insertSql = "?,?,?"+lenOfIndicat*",?"
        for stk in allStks:
            print stk
            date = self.compStks.GetIncludeAndExcludeDate(stk,'000300')
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = calcFinDeriv.GetReportDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                acctPeriod = ""
                val = []
                for indic in self.indicators:
                    indicVal = calcFinDeriv.Calc(dt,300,stk,indic)
                    if indicVal == None:
                        acctPeriod = None
                        _val = None
                    else:
                        acctPeriod = indicVal[0]
                        _val = indicVal[1]
                    val.append(_val)
                row = [stk,acctPeriod,dt]
                for v in val:
                    row.append(v)
                cur.execute("INSERT INTO FinRptDerivData VALUES ({})".format(insertSql),tuple(row))
        self.indicConn.commit()
                    
            
            

    