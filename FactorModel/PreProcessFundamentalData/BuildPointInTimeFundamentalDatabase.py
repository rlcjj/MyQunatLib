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
import FactorModel.PreProcessFundamentalData._GetPointInTimeData as GetPITData
import Tools.LogOutputHandler as LogHandler


########################################################################
class BuildPITFundamentalDatabase(object):
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
        self.objConstituentStocks = GetIndexConstituentStocks.GetIndexConstituentStocks(indexConstituentDatabase,self.logger)       


    #----------------------------------------------------------------------
    def LoadFundamentalDataItemsToBeProcessed(self):
        """
        加载需要预处理的财务报表和预测数据项目
        """
        
        itemDirPath = root+"//FactorModel//PreProcessFundamentalData//DataItemToBeProcessed"
        
        finItemFilePath = itemDirPath+"//FinancialReportData"
        self.finItems = []
        finItemFiles = os.listdir(finItemFilePath)
        for f in finItemFiles:
            itemName = f.split('.')
            if itemName[0][0]!='_' and itemName[1]=="py":
                self.logger.info("<{}>-Load financial report item {}".format(__name__.split('.')[-1],itemName[0]))
                self.finItems.append(itemName[0])
        
        fcstItemFilePath = itemDirPath+"//ForecastReportData"
        self.fcstItems = []
        fcstItemFiles = os.listdir(fcstItemFilePath)
        for f in fcstItemFiles:
            itemName = f.split('.')
            if itemName[0][0]!='_' and itemName[1]=="py":
                self.logger.info("<{}>-Load forecast report item {}".format(__name__.split('.')[-1],itemName[0]))
                self.fcstItems.append(itemName[0])        

    
    #----------------------------------------------------------------------
    def CreateDatabase(self,pointInTimeDatabaseName):
        """
        创立时点基本面数据数据库
        """
        pttDbAddr = self.locDbPath["ProcEquity"]+pointInTimeDatabaseName
        self.pttConn = lite.connect(pttDbAddr)
        cur = self.pttConn.cursor()
        cur.execute("DROP TABLE IF EXISTS FinancialPointInTimeData")
        cur.execute("DROP TABLE IF EXISTS ForecastPointInTimeData")
        cur.execute("PRAGMA synchronous = OFF")
        
        self.logger.info("<{}>-Create data table 'FinancialPointInTimeData'".format(__name__.split('.')[-1]))
        sqlStr = ""
        for item in self.finItems:
            sqlStr+=","+item+" FLOAT"
        cur.execute("""
                    CREATE TABLE FinancialPointInTimeData(StkCode TEXT,
                                                         AcctPeriod TEXT,
                                                         DeclareDate TEXT,
                                                         ReportType INT,
                                                         IsNewAcctRule INT,
                                                         IsListed INT
                                                         {})
                    """.format(sqlStr))
        
        self.logger.info("<{}>-Create data table 'ForecastPointInTimeData'".format(__name__.split('.')[-1]))
        sqlStr = ""
        for item in self.fcstItems:
            sqlStr+=","+item+" FLOAT"
        cur.execute("""
                    CREATE TABLE ForecastPointInTimeData(StkCode TEXT,
                                                         AcctPeriod TEXT,
                                                         DeclareDate TEXT
                                                         {})
                    """.format(sqlStr))        
        
    
    #----------------------------------------------------------------------
    def CalculateAndSaveData(self,startDate):
        """
        计算并存储基本面时点数据
        """
        #原始财务报表和预测数据数据库地址
        finRptDbPath = self.locDbPath["RawEquity"]+"FinRptData\\FinRptData_Wind_CICC.db"
        mktDataDbPath = self.locDbPath["RawEquity"]+"MktData\\MktData_Wind_CICC.db"
        
        #创建获取时点数据的类
        getPITDataCls = GetPITData.GetPointInTimeData(finRptDbPath,mktDataDbPath,self.logger)
        allStkCodes = self.objConstituentStocks.GetAllStocksExcludedAfterGivenDate(startDate,self.constituentIndexCode)
        
        #处理财务报表数据
        cur = self.pttConn.cursor()
        lenOfItems = len(self.finItems)
        insertSql = "?,?,?,?,?,?"+lenOfItems*",?"
        for stk in allStkCodes:
            self.logger.info("<{}>-Process financial report data - stock code {}".format(__name__.split('.')[-1],stk))
            date = self.objConstituentStocks.GetStockIncludedAndExcludedDate(stk,self.constituentIndexCode)
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = getPITDataCls.GetFinancialReportDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                itemVals = getPITDataCls.ProcessFinancialData(dt,300,stk,self.finItems)
                if itemVals!=None:
                    row = [stk,itemVals[0],dt,itemVals[1],itemVals[2],itemVals[3]]
                    for itemVal in itemVals[4]:
                        row.append(itemVal)
                    cur.execute("INSERT INTO FinancialPointInTimeData VALUES ({})".format(insertSql),tuple(row))
        self.pttConn.commit()
        
        #处理预测报告数据
        cur = self.pttConn.cursor()
        lenOfItems = len(self.fcstItems)
        insertSql = "?,?,?"+lenOfItems*",?"
        for stk in allStkCodes:
            self.logger.info("<{}>-Process forecast report data - stock code {}".format(__name__.split('.')[-1],stk))
            date = self.objConstituentStocks.GetStockIncludedAndExcludedDate(stk,self.constituentIndexCode)
            begDate = date[0][0]
            endDate = date[-1][1]
            rptDeclareDate = getPITDataCls.GetForecastReportDeclareDate(stk,begDate,endDate)
            for dt in rptDeclareDate:
                itemVals = getPITDataCls.ProcessForecastData(dt,300,stk,self.fcstItems)
                if itemVals!=None:
                    row = [stk,itemVals[0],dt]
                    for itemVal in itemVals[1]:
                        row.append(itemVal)
                    cur.execute("INSERT INTO ForecastPointInTimeData VALUES ({})".format(insertSql),tuple(row))
        self.pttConn.commit()        
                    
            
            

    