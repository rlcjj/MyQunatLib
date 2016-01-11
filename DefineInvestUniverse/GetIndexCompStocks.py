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


########################################################################
class GetIndexCompStocks(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,dbAddress):
        """Constructor"""
        locDbPath = GetPath.GetLocalDatabasePath()
        lite.register_adapter(decimal.Decimal, lambda x:str(x))
        self.conn = lite.connect(locDbPath["RawEquity"]+dbAddress)
        self.conn.text_factory = str
        
    #----------------------------------------------------------------------
    def GetStocks(self,date,*indexCode):
        """"""
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
    def GetAllStocks(self,*indexCode):
        """"""
        cur = self.conn.cursor()
        sql = """
                  SELECT DISTINCT StkCode
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
        stks = []
        for row in rows:
            stks.append(row[0])
        return stks        
        
            
    #----------------------------------------------------------------------
    def GetIndexAdjustDate(self,*indexCode):
        """"""
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
    def GetIncludeAndExcludeDate(self,stkCode,indexCode):
        """"""
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