#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/23
"""

import os,numpy
import sqlite3 as lite
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import UpdateFactorDatabase.FundamentalFactors._CalculateFactorValues as CalcFactorVals
import Tools.LogOutputHandler as LogHandler
import Configs.RootPath as Root
RootPath = Root.RootPath


########################################################################
class GetFactorValue(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("ComputeFactorsAndZScores")
        else:    
            self.logger = logger
            
    
    #----------------------------------------------------------------------
    def LoadFactorTableIntoMemory(self,dbNameFactor,factorTypes):
        """"""
        dbPath = GetPath.GetLocalDatabasePath()["EquityDataRefined"]
        dbPath = dbPath + dbNameFactor
        self.conn1 = lite.connect(dbPath)
        self.conn1.text_factory = str
        self.conn2 = lite.connect(":memory:")
        self.conn2.text_factory = str
        cur = self.conn2.cursor()
        cur.execute("ATTACH '{}' AS FVData").format(dbPath)
        
        if "Fundamental" in factorTypes:
            cur.execute("CREATE FundamentalFactors AS SELECT * FROM FVData.FundamentalFactors")
        if "Technical" in factorTypes:
            cur.execute("CREATE TechnicalFactors AS SELECT * FROM FVData.TechnicalFactors")
        if "Analyst" in factorTypes:
            cur.execute("CREATE AnalystFactors AS SELECT * FROM FVData.AnalystFactors")
        
    
    
    #----------------------------------------------------------------------
    def GetFactorValueFromMemory(self,factorType,factorName,date,effectiveTime):
        """"""
        if factorType == "Fundamental":
            if "Fundamental" in factorTypes:
                conn = self.conn2
            else:
                conn = self.conn1
        if factorType == "Technical":
            if "Technical" in factorTypes:
                conn = self.conn2
            else:
                conn = self.conn1        
        if factorType == "Analyst":
            if "Analyst" in factorTypes:
                conn = self.conn2
            else:
                conn = self.conn1           
        
        
        
    
    
