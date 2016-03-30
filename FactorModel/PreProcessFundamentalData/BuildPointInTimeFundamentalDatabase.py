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
import InvestmentUniverse.GetIndexConstituentStocks as GetIndexConstituentStocks
import FactorModel.PreProcessFundamentalData._GetPointInTimeData as GetPTTData
import Tools.LogOutputHandler as LogHandler


########################################################################
class BuildFundamentalDatabase(object):
    """
    整合时点财务数据写入本地数据库
    """

    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("SyncFinRpt.log")
        else:    
            self.logger = logger        
        
        self.locDbPath = GetPath.GetLocalDatabasePath()
        
        
    #----------------------------------------------------------------------
    def SetStockUniverse(self,indexConstituentDatabase,constituentIndexCode):
        """
        确定股票投资范围
        """
        self.constituentIndexCode = constituentIndexCode
        self.constituentStockCls = GetIndexConstituentStocks.GetIndexConstituentStocks(dbAddress,self.logger)       


    #----------------------------------------------------------------------
    def LoadFundamentalDataItemsToBeProcessed(self):
        """
        加载需要预处理的财务报表和预测数据项目
        """
        itemDirPath = root+"//FactorModel//PreProcessFundamentalData//DataProcessAlgo"
        
        finItemFilePath = itemDirPath+"//FinancialReportData"
        self.finItems = []
        finItemFiles = os.listdir(finItemFilePath)
        for f in finItemFiles:
            itemName = f.split('.')
            if itemName[0][0]!='_' and itemName[1]=="py":
                self.logger.info("Load financial report item {}".format(itemName[0]))
                self.finItems.append(itemName[0])
        
        fcstItemFilePath = itemDirPath+"//ForecastReportData"
        self.fcstItems = []
        fcstItemFiles = os.listdir(fcstItemFilePath)
        for f in fcstItemFiles:
            itemName = f.split('.')
            if itemName[0][0]!='_' and itemName[1]=="py":
                self.logger.info("Load forecast report item {}".format(itemName[0]))
                self.fcstItems.append(itemName[0])        

    
    #----------------------------------------------------------------------
    def CreateDatabase(self,pointInTimeDatabaseName):
        """
        创立即时基本面数据数据库
        """
        pttDbAddr = self.locDbPath["ProcEquity"]+pointInTimeDatabaseName
        self.pttConn = lite.connect(pttDbAddr)
        cur = self.pttConn.cursor()
        cur.execute("DROP TABLE IF EXISTS FinancialPontInTimeData")
        cur.execute("DROP TABLE IF EXISTS ForecastPointInTimeData")
        cur.execute("PRAGMA synchronous = OFF")
        
        sqlStr = ""
        for item in self.finItems:
            sqlStr+=","+item+" FLOAT"
        
        cur.execute("""
                    CREATE TABLE FinancialPoitInTiemData(StkCode TEXT,
                                                         AcctPeriod TEXT,
                                                         DeclareDate TEXT,
                                                         ReportType INT,
                                                         IsNewAcctRule INT,
                                                         IsListed INT
                                                         {})
                    """.format(sqlStr))
        
        sqlStr = ""
        for item in self.fcstItems:
            sqlStr+=","+item+" FLOAT"
        cur.execute("""
                    CREATE TABLE ForecastPointInTImeData(StkCode TEXT,
                                                         AcctPeriod TEXT,
                                                         DeclareDate TEXT
                                                         {})
                    """.format(sqlStr))        
        
    
    #----------------------------------------------------------------------
    def GenerateData(self):
        """
        计算并存储基本面即时数据
        """
        finRptDbPath = self.locDbPath["RawEquity"]+"FinRptData\\FinRptData_Wind_CICC.db"
        mktDataDbPath = self.locDbPath["RawEquity"]+"MktData\\MktData_Wind_CICC.db"
        fdmtData = FdmtData.GetFdmtDerivData(finRptDbPath,mktDataDbPath)
        allStks = self.compStks.GetAllStocks(self.indexCode)
        
        cur = self.indicConn.cursor()
        lenOfItems = len(self.items1)
        insertSql = "?,?,?,?,?,?"+lenOfItems*",?"
        for stk in allStks:
            print stk
            date = self.compStks.GetIncludeAndExcludeDate(stk,self.indexCode)
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = fdmtData.GetFinDataDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                itemVals = fdmtData.CalcFinRptDerivData(dt,300,stk,self.items1)
                if itemVals!=None:
                    row = [stk,itemVals[0],dt,itemVals[1],itemVals[2],itemVals[3]]
                    for itemVal in itemVals[4]:
                        row.append(itemVal)
                    cur.execute("INSERT INTO FinRptDerivData VALUES ({})".format(insertSql),tuple(row))
        self.indicConn.commit()
        
        cur = self.indicConn.cursor()
        lenOfItems = len(self.items2)
        insertSql = "?,?,?"+lenOfItems*",?"
        for stk in allStks:
            print stk
            date = self.compStks.GetIncludeAndExcludeDate(stk,self.indexCode)
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = fdmtData.GetForecastDataDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                itemVals = fdmtData.CalcForecastDerivData(dt,300,stk,self.items2)
                if itemVals!=None:
                    row = [stk,itemVals[0],dt]
                    for itemVal in itemVals[1]:
                        row.append(itemVal)
                    cur.execute("INSERT INTO ForecastData VALUES ({})".format(insertSql),tuple(row))
        self.indicConn.commit()        
                    
            
            

    