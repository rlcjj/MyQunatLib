#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/23
"""

import os,numpy
from datetime import datetime,timedelta
import sqlite3 as lite
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import UpdateFactorDatabase.FundamentalFactors._CalculateFactorValues as CalcFactorVals
import Tools.LogOutputHandler as LogHandler
import Configs.RootPath as Root
RootPath = Root.RootPath


########################################################################
class GetFactorValues(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("ComputeFactorsAndZScores")
        else:    
            self.logger = logger
        self.trdDays = GetTrdDay.GetTradeDays()  
            
    
    #----------------------------------------------------------------------
    def LoadFactorTablesIntoMemory(self,dbNameFactor,factorTypes):
        """"""
        dbPath = GetPath.GetLocalDatabasePath()["EquityDataRefined"]
        dbPath = dbPath + dbNameFactor
        self.conn1 = lite.connect(dbPath)
        self.conn1.text_factory = str
        self.conn2 = lite.connect(":memory:")
        self.conn2.text_factory = str
        cur = self.conn2.cursor()
        cur.execute("ATTACH '{}' AS FVData".format(dbPath))
        
        self.fundamentalConn = self.conn1
        self.technicalConn = self.conn1
        self.analystConn = self.conn1
        
        if "Fundamental" in factorTypes:
            cur.execute("CREATE TABLE FinancialPITData AS SELECT * FROM FVData.FinancialPointInTimeData")
            cur.execute("CREATE INDEX idF ON FinancialPITData(StkCode,DeclareDate)")
            self.fundamentalConn = self.conn2            
        if "Technical" in factorTypes:
            cur.execute("CREATE TABLE TechnicalFactors AS SELECT * FROM FVData.TechnicalFactors")
            cur.execute("CREATE INDEX idT ON TechnicalFactors(StkCode,Date)")
            self.technicalConn = self.conn2
        if "Analyst" in factorTypes:
            cur.execute("CREATE TABLE AnalystFactors AS SELECT * FROM FVData.AnalystFactors")
            cur.execute("CREATE INDEX idA ON AnalystFactors(StkCode,Date)")
            self.analystConn = self.conn2
    
    #----------------------------------------------------------------------
    def ChooseFactors(self,fundamentals,technicals,analysts):
        """"""
        self.fundamentalFactors = fundamentals
        self.technicalFactors = technicals
        self.analystFactors = analysts
        
        
    #----------------------------------------------------------------------
    def GetFutureReturns(self,stkCode,date,horizon):
        """"""
        curTech = self.technicalConn.cursor()
        p = self.trdDays.index(date)
        _date = self.trdDays[p+horizon]
        sql = """
              SELECT ClosePrice_Adj 
              FROM TechnicalFactors
              WHERE StkCode='{}'
              AND Date in ('{}','{}')
              ORDER BY Date ASC
              """
        curTech.execute(sql.format(stkCode,date,_date))
        rows = curTech.fetchall()
        if len(rows)<2:
            return numpy.nan
        else:
            return (rows[1][0]-rows[0][0])/rows[0][0]
        
        
    #----------------------------------------------------------------------
    def GetHistoricalReturns(self,stkCode,date,horizon):
        """"""
        curTech = self.technicalConn.cursor()
        p = self.trdDays.index(date)
        _date = self.trdDays[p-horizon]
        sql = """
                  SELECT ClosePrice_Adj 
                  FROM TechnicalFactors
                  WHERE StkCode='{}'
                  AND Date in ('{}','{}')
                  ORDER BY Date ASC
                  """
        curTech.execute(sql.format(stkCode,date,_date))
        rows = curTech.fetchall()
        if len(rows)<2:
            return numpy.nan
        else:
            return (rows[1][0]-rows[0][0])/rows[0][0]        
        
    
    
    #----------------------------------------------------------------------
    def GetFactorValues(self,stkCode,date,effectiveTime):
        """"""
        curFdmt = self.fundamentalConn.cursor()
        curTech = self.technicalConn.cursor()
        curAnal = self.analystConn.cursor()

        factorValues = {}
        factorDefs = {}
        for i in xrange(len(self.fundamentalFactors)):
            #import FactorModel.FactorDef.S2P_TTM as algo
            exec("import FactorModel.FactorDef.{} as algo".format(self.fundamentalFactors[i]))
            factorDefs[self.fundamentalFactors[i]] = algo
        
        if len(self.fundamentalFactors)>0:
            _lookupDate = datetime.strptime(date,"%Y%m%d")
            _lookupLimit = _lookupDate - timedelta(days=effectiveTime)
            lookupLimit = _lookupLimit.strftime("%Y%m%d")    
            date = (lookupLimit,date)        
            begDate = date[0] 
            endDate = date[1]  
            
            sql0 = """
                          SELECT ClosePrice,TotalCapital
                          FROM TechnicalFactors
                          WHERE StkCode='{}'
                                AND Date='{}' 
                          """   
            curTech.execute(sql0.format(stkCode,endDate))
            content = curTech.fetchone() 
            stkInfo = content
            if stkInfo == None:
                p = numpy.nan
                s = numpy.nan
            else:
                p = stkInfo[0]
                s = stkInfo[1]
            factorValues["ClosePrice"] = p
            factorValues["TotalCapital"] = s
                
            
            sql1 = """
                  SELECT AcctPeriod,DeclareDate,ReportType
                  FROM FinancialPITData
                  WHERE StkCode='{}'
                      AND DeclareDate>='{}'
                      AND DeclareDate<='{}'
                  ORDER BY AcctPeriod DESC LIMIT 1
                  """
            curFdmt.execute(sql1.format(stkCode,begDate,endDate))
            content = curFdmt.fetchone()
            rptInfo = content
            factorValues["Date"]=endDate
            if rptInfo == None:
                for i in xrange(len(self.fundamentalFactors)):
                    factorValues[self.fundamentalFactors[i]]=numpy.nan
                    factorValues["FinYear"] = ''
                    factorValues["AnnouceDate"] = ''
                    factorValues["RptType"] = None
            else:
                for i in xrange(len(self.fundamentalFactors)):
                    factorValues["FinYear"] = rptInfo[0]
                    factorValues["AnnouceDate"] = rptInfo[1]
                    factorValues["RptType"] = rptInfo[2]     
                    factorVal = factorDefs[self.fundamentalFactors[i]].Calc(curFdmt,rptInfo,p,s,date,stkCode)
                    if factorVal==None:
                        factorValues[self.fundamentalFactors[i]] = numpy.nan
                    else:
                        factorValues[self.fundamentalFactors[i]]=factorVal
                    
        if len(self.technicalFactors)>0:
            sqlStr = ""
            for fct in self.technicalFactors:
                sqlStr += ','+fct 
            sql2 = """
                  SELECT Date {}
                  FROM TechnicalFactors
                  WHERE StkCode='{}'
                        AND Date='{}' 
                  """   
            curTech.execute(sql2.format(sqlStr,stkCode,date[1]))
            content = curTech.fetchone()
            if content == None:
                for i in xrange(len(self.technicalFactors)):
                    factorValues[self.technicalFactors[i]]=numpy.nan
            else:
                for i in xrange(len(self.technicalFactors)):
                    if content[i+1] == None:
                        factorValues[self.technicalFactors[i]]=numpy.nan
                    else:
                        factorValues[self.technicalFactors[i]]=content[i+1]                    
                    
        return factorValues
            

            