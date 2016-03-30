#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/11/30
"""

import os,sys,logging
root = os.path.abspath("D:\\MyQuantLib\\")
sys.path.append(root)

import sqlite3 as lite
import time
from datetime import datetime,timedelta
import Tools.LogOutputHandler as LogHandler


########################################################################
class GetPointInTimeData(object):
    """
    获取当前时点的最新基本面数据
    """

    #----------------------------------------------------------------------
    def __init__(self,finRptDbPath,mktDataDbPath,logger=None):
        """
        Constructor
        Load the fundamental data into in-memory database
        """
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("SyncFinRpt.log")
        else:    
            self.logger = logger
        
        self.logger.info("Create in-memory database")
        self.conn = lite.connect(":memory:")
        self.conn.text_factory = str
        cur = self.conn.cursor()
        
        self.logger.info("Load local database into in-memory database")
        cur.execute("ATTACH '{}' AS FinRpt".format(finRptDbPath))
        cur.execute("ATTACH '{}' AS MktData".format(mktDataDbPath))
        cur.execute("CREATE TABLE BalanceSheet AS SELECT * FROM FinRpt.BalanceSheet")
        cur.execute("CREATE TABLE IncomeStatement AS SELECT * FROM FinRpt.IncomeStatement")
        cur.execute("CREATE TABLE CashFlowStatement AS SELECT * FROM FinRpt.CashFlowStatement")
        cur.execute("CREATE TABLE ForecastData AS SELECT * FROM FinRpt.ForecastData")        
        cur.execute("CREATE TABLE Dividend AS SELECT * FROM MktData.Dividend")
        self.logger.info("Finished")
        
        self.logger.info("Create index on in-memory database")
        cur.execute("CREATE INDEX Id1 ON BalanceSheet (StkCode,RPT_DATE,RDeclareDate)")
        cur.execute("CREATE INDEX Id2 ON IncomeStatement (StkCode,RPT_DATE,RDeclareDate)")
        cur.execute("CREATE INDEX Id3 ON CashFlowStatement (StkCode,RPT_DATE,RDeclareDate)")
        cur.execute("CREATE INDEX IdF ON ForecastData (StkCode,RPT_DATE,RDeclareDate)")
        cur.execute("CREATE INDEX IdD ON Dividend (StkCode,RDeclareDate)")
        self.logger.info("Finished")
    
    
    #----------------------------------------------------------------------
    def GetAllStockCode(self):
        """
        获取数据库所有股票代码
        """
        self.logger.info("Get all ticker code in the database")
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT StkCode FROM BalanceSheet")
        rows = cur.fetchall()
        allStks = []
        for row in rows:
            allStks.append(row[0])
        return allStks
    
    #----------------------------------------------------------------------
    def GetFinancialReportDeclareDate(self,stkCode,begDate,endDate):
        """
        获取财务报表的公告日期
        """
        if endDate == None:
            endDate = "20200101" #Some day in the far future
        _rBegDate = datetime.strptime(begDate,"%Y%m%d")-timedelta(days=180)
        _rEndDate = datetime.strptime(endDate,"%Y%m%d")+timedelta(days=180)
        rBegDate = _rBegDate.strftime("%Y%m%d")
        rEndDate = _rEndDate.strftime("%Y%m%d")
        cur = self.conn.cursor()
        declareDates = []
        for tb in ["BalanceSheet","IncomeStatement","Dividend"]:
            cur.execute("""
                        SELECT DISTINCT RDeclareDate 
                        FROM {} 
                        WHERE StkCode='{}' 
                        AND RDeclareDate>='{}'
                        AND RDeclareDate<='{}'
                        ORDER BY RDeclareDate ASC
                        """.format(tb,stkCode,rBegDate,rEndDate))
            rows = cur.fetchall()
            _declareDates = []
            for row in rows:
                _declareDates.append(row[0])
            declareDates=list(set(declareDates)|set(_declareDates))
        declareDates.sort()
        return declareDates      
    
    #----------------------------------------------------------------------
    def GetForecastReportDeclareDate(self,stkCode,begDate,endDate):
        """
        获取预测报告公告日期
        """
        if endDate == None:
            endDate = "20200101" #Some day in the far future
        _rBegDate = datetime.strptime(begDate,"%Y%m%d")-timedelta(days=180)
        _rEndDate = datetime.strptime(endDate,"%Y%m%d")+timedelta(days=180)
        rBegDate = _rBegDate.strftime("%Y%m%d")
        rEndDate = _rEndDate.strftime("%Y%m%d")
        cur = self.conn.cursor()
        declareDates = []
        for tb in ["ForecastData"]:
            cur.execute("""
                        SELECT DISTINCT RDeclareDate 
                        FROM {} 
                        WHERE StkCode='{}' 
                        AND RDeclareDate>='{}'
                        AND RDeclareDate<='{}'
                        ORDER BY RDeclareDate ASC
                        """.format(tb,stkCode,rBegDate,rEndDate))
            rows = cur.fetchall()
            _declareDates = []
            for row in rows:
                _declareDates.append(row[0])
            declareDates=list(set(declareDates)|set(_declareDates))
        declareDates.sort()
        return declareDates           
    
    
    #----------------------------------------------------------------------
    def ProcessFinancialData(self,lookupDate,lagDays,stkCode,items):
        """
        初步处理财报数据，获取公布时间点最新数据，并根据算法简单计算
        """
        
        cur = self.conn.cursor()
        
        _lookupDate = datetime.strptime(lookupDate,"%Y%m%d")
        _lookupLimit = _lookupDate - timedelta(days=lagDays)
        lookupLimit = _lookupLimit.strftime("%Y%m%d")    
        sql = """
              SELECT RPT_DATE,CompanyType,IsNewRule,IsListed 
              FROM BalanceSheet 
              WHERE StkCode='{}' AND RPT_DATE>'{}' AND RPT_DATE<'{}'
              AND RDeclareDate<='{}'
              ORDER BY RPT_DATE+RDeclareDate DESC LIMIT 1
              """
        cur.execute(sql.format(stkCode,lookupLimit,lookupDate,lookupDate))
        MyPrint(sql.format(stkCode,lookupLimit,lookupDate,lookupDate))
        content = cur.fetchone()
        if content==None:
            return None
        rptInfo = content
        derivData = []
        for item in items:
            exec("import FactorModel.PreProcessFundamentalData.FinancialReportData.{} as _item".format(item))
            _derivData = _item.Calc(cur,lookupDate,rptInfo,stkCode)
            derivData.append(_derivData)
        return rptInfo[0],rptInfo[1],rptInfo[2],rptInfo[3],derivData
    
    
    #----------------------------------------------------------------------
    def ProcessForecastData(self,lookupDate,lagDays,stkCode,items):
        """
        初步处理预测数据，获取公布时间点最新数据，并根据算法简单计算
        """
        tm1 = time.time()
        cur = self.conn.cursor()
        derivData = []
        for item in items:
            exec("import FactorModel.PreProcessFundamentalData.ForecastReportData.{} as _item".format(item))
            _derivData = _item.Calc(cur,lookupDate,"",stkCode)
            derivData.append(_derivData)
        tm2 = time.time()
        thisAcctYear = lookupDate[0:4]+"1231"
        #print "Time consume:{}".format(tm2-tm1)
        return thisAcctYear,derivData    
    
