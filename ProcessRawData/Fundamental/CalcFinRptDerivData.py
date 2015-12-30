#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/11/30
"""

import sqlite3 as lite
import time
from datetime import datetime,timedelta

########################################################################
class CalcFinRptDerivData(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finRptDbPath,mktDataDbPath):
        """Constructor"""
        self.conn = lite.connect(":memory:")
        self.conn.text_factory = str
        cur = self.conn.cursor()
        print "Load local database into in-memory database"
        cur.execute("ATTACH '{}' AS FinRpt".format(finRptDbPath))
        cur.execute("ATTACH '{}' AS MktData".format(mktDataDbPath))
        cur.execute("CREATE TABLE BalanceSheet AS SELECT * FROM FinRpt.BalanceSheet")
        cur.execute("CREATE TABLE IncomeStatement AS SELECT * FROM FinRpt.IncomeStatement")
        cur.execute("CREATE TABLE CashFlowStatement AS SELECT * FROM FinRpt.CashFlowStatement")
        cur.execute("CREATE TABLE Divident AS SELECT * FROM MktData.Divident")
        print "Finished"
        print "Create index"
        cur.execute("CREATE INDEX Id1 ON BalanceSheet (rpt_date,rpttype,stkCode,rdeclaredate)")
        cur.execute("CREATE INDEX Id2 ON IncomeStatement (rpt_date,rpttype,stkCode,rdeclaredate)")
        cur.execute("CREATE INDEX Id3 ON CashFlowStatement (rpt_date,rpttype,stkCode,rdeclaredate)")
        cur.execute("CREATE INDEX IdD ON Divident (Date)")
        print "Finished"
        cur.execute("select * from divident ")
        a = cur.fetchone()
        print a
    
    #----------------------------------------------------------------------
    def GetAllStocks(self):
        """"""
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT StkCode FROM BalanceSheet")
        rows = cur.fetchall()
        allStks = []
        for row in rows:
            allStks.append(row[0])
        return allStks
    
    #----------------------------------------------------------------------
    def GetReportDeclareDate(self,stkCode,begDate,endDate):
        """"""
        if endDate == None:
            endDate = "20200101" #Some day in the far future
        _rBegDate = datetime.strptime(begDate,"%Y%m%d")-timedelta(days=180)
        _rEndDate = datetime.strptime(endDate,"%Y%m%d")+timedelta(days=180)
        rBegDate = _rBegDate.strftime("%Y%m%d")
        rEndDate = _rEndDate.strftime("%Y%m%d")
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT DISTINCT RDeclareDate 
                    FROM CashFlowStatement 
                    WHERE StkCode='{}' 
                          AND RDeclareDate>='{}'
                          AND RDeclareDate<='{}'
                    ORDER BY RDeclareDate ASC
                    """.format(stkCode,rBegDate,rEndDate))
        rows = cur.fetchall()
        declareDate = []
        for row in rows:
            declareDate.append(row[0])
        return declareDate        
        
    
    #----------------------------------------------------------------------
    def Calc(self,lookupDate,lagDays,stkCode,algo):
        """"""
        tm1 = time.time()
        
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

        indicator = algo.Calc(cur,lookupDate,rptInfo,stkCode)
        tm2 = time.time()
        #print "Time consume:{}".format(tm2-tm1)
        return indicator
    
#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    