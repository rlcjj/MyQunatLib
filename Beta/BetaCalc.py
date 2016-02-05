#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/2/3
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath


########################################################################
class BetaCalc(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,mktDataDbName,ifLoadMemoryDb,startDate):
        """Constructor"""
        mktDbPath = GetPath.GetLocalDatabasePath()["RawEquity"]+mktDataDbName
        if ifLoadMemoryDb==1:
            self.conn = lite.connect(":memory:")
            self.conn.text_factory = str
            cur = self.conn.cursor()
            print "Load market data into memory database"
            cur.execute("ATTACH '{}' AS MktData".format(mktDbPath))
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,TC FROM MktData.A_Share_Data WHERE Date>='{}'".format(startDate))
            cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,TC FROM MktData.IndexData WHERE Date>='{}'".format(startDate))
            print "Done"
            print "Create index on table StockData and table IndexData"
            cur.execute("CREATE INDEX idS ON StockData (StkCode,Date)")
            cur.execute("CREATE INDEX idI ON IndexData (StkCode,Date)")
            print "Done"
        else:
            print "Directly connect to local database"
            self.conn = lite.connect(mktDbPath)
            self.conn.text_factory = str
            
            
    #----------------------------------------------------------------------
    def SetParameters(self,marketIndex,returnHorizon,lookbackWindow):
        """"""
        self.mktIndex = marketIndex
        self.returnHorizon = returnHorizon
        self.lookbackWindow = lookbackWindow
        
    #----------------------------------------------------------------------
    def Calc(self,date,stock):
        """"""
        tm1 = time.time()
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT _Index.Date,_Index.TC,_Stock.TC From
                       (SELECT Date,TC 
                        FROM IndexData 
                        WHERE StkCode='{}' AND Date<='{}' 
                        ORDER by DATE DESC LIMIT {})_Index
                    LEFT JOIN 
                       (SELECT Date,TC 
                        FROM StockData 
                        WHERE StkCode='{}' AND Date<='{}' 
                        ORDER by DATE DESC LIMIT {})_Stock
                    ON _Index.Date=_Stock.Date
                    """.format(self.mktIndex,date,self.lookbackWindow+1,stock,date,self.lookbackWindow+1))
        vals = cur.fetchall()
        tm2 = time.time()
        print tm2-tm1
        totalDate = []
        indexDailyPrice = []
        for v in vals:
            print v
            totalDate.append(v[0])
            indexDailyPrice.append(v[1])
        
  
            
            
if __name__ == "__main__":
    betaCalc = BetaCalc("MktData\\MktData_Wind_CICC.db", 1,"20100127")
    betaCalc.SetParameters("000300",5,200)
    betaCalc.Calc("20160128","600837")
        
        
            
        
    