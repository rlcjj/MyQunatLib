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


########################################################################
class GetIndexConstituentStocks(object):
    """
    获取指数成分股
    """

    #----------------------------------------------------------------------
    def __init__(self,dbAddress,logger=None):
        """Constructor"""
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("SyncFinRpt.log")
        else:    
            self.logger = logger             
        
        locDbPath = GetPath.GetLocalDatabasePath()
        lite.register_adapter(decimal.Decimal, lambda x:str(x))
        self.conn = lite.connect(":memory:")
        cur = self.conn.cursor()
        self.conn.text_factory = str
        cur.execute("ATTACH '{}' AS _IndexComp".format(locDbPath["RawEquity"]+dbAddress))
        self.logger.info("Load table IndexComp")
        cur.execute("CREATE TABLE IndexComp AS SELECT * FROM _IndexComp.IndexComp")
        self.logger.info("Load table SWIndustry1st")
        cur.execute("CREATE TABLE SWIndustry1st AS SELECT * FROM _IndexComp.SWIndustry1st")        
        self.logger.info("Create index on IndexComp")
        cur.execute("CREATE INDEX idI ON IndexComp (IndexCode,IncDate,ExcDate,StkCode)")
        self.logger.info("Create index on SWIndustry1st")
        cur.execute("CREATE INDEX idS ON SWIndustry1st (IndusCode,IncDate,ExcDate,StkCode)")
        
        
    #----------------------------------------------------------------------
    def GetConstituentStocksAtGivenDate(self,date,*indexCode):
        """
        获取给定日期指数所包含的的成分股
        """
        cur = self.conn.cursor()
        sql = """
              SELECT StkCode
              FROM IndexComp
              WHERE IncDate<={}
              AND (ExcDate>={} or ExcDate is NULL)
              AND IndexCode in {}
              """
        _indexCode = []
        for item in indexCode:
            _indexCode.append(item)
        _indexCode.append("None")
        #print sql.format(date,date,tuple(_indexCode))
        cur.execute(sql.format(date,date,tuple(_indexCode)))
        rows = cur.fetchall()
        stks = []
        for row in rows:
            stks.append(row[0])
        return stks
    
    
    #----------------------------------------------------------------------
    def GetAllStocksIncludedAfterGivenDate(self,startDate,*indexCode):
        """
        获取给定日期后纳入指数的所有股票(包含已经剔除的)
        """
        cur = self.conn.cursor()
        sql = """
                  SELECT DISTINCT StkCode
                  FROM IndexComp
                  WHERE IndexCode in {}
                  AND (ExcDate>'{}' OR ExcDate IS NULL)
                  """
        _indexCode = []
        for item in indexCode:
            _indexCode.append(item)
        _indexCode.append("None")
        #print sql.format(tuple(_indexCode))
        cur.execute(sql.format(tuple(_indexCode),startDate))
        rows = cur.fetchall()
        stks = []
        for row in rows:
            stks.append(row[0])
        return stks      
          
            
    #----------------------------------------------------------------------
    def GetIndexAdjustDate(self,*indexCode):
        """
        获取指数调整日期
        """
        cur = self.conn.cursor()
        sql = """
              SELECT DISTINCT IncDate
              FROM IndexComp
              WHERE IndexCode in {}
              """     
        _indexCode = []
        for item in indexCode:
            _indexCode.append(item)
        _indexCode.append("None")
        #print sql.format(tuple(_indexCode))
        cur.execute(sql.format(tuple(_indexCode)))
        rows = cur.fetchall()
        date = []
        for row in rows:
            date.append(row[0])
        return date
    
    
    #----------------------------------------------------------------------
    def GetStockIncludedAndExcludedDate(self,stkCode,indexCode):
        """
        获取股票纳入指数和剔除出指数日期
        """
        cur = self.conn.cursor()
        sql = """
              SELECT IncDate,ExcDate
              FROM IndexComp
              WHERE StkCode='{}'
              AND IndexCode='{}'
              ORDER BY IncDate asc
              """
        #print sql.format(indexCode,stkCode)
        cur.execute(sql.format(stkCode,indexCode))
        rows = cur.fetchall()
        date = []
        if rows!=None:
            for row in rows:
                date.append(row)
            return date
        else:
            return None
        
        
    #----------------------------------------------------------------------
    def GetStockNameAndIndustry(self,stkCode,date):
        """
        获取股票的中文名和所属行业名称
        """
        cur = self.conn.cursor()
        sql = """
              SELECT StkName,IndusCode,IndusName
              FROM SWIndustry1st
              WHERE StkCode='{}'
              AND IncDate<='{}'
              AND (ExcDate ISNULL or ExcDate>='{}')
              """
        cur.execute(sql.format(stkCode,date,date))
        row = cur.fetchone()
        return row