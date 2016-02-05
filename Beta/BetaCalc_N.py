#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/2/3
"""

import os,sys,logging ,time,decimal,codecs,numpy 
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
            cur.execute("CREATE TABLE StockData AS SELECT StkCode,Date,TC_Adj FROM MktData.A_Share_Data WHERE Date>='{}'".format(startDate))
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
    def SetParameters(self,marketIndex,stockUniverse,returnHorizon,lookbackWindow):
        """"""
        self.mktIndex = marketIndex
        self.returnHorizon = returnHorizon
        self.lookbackWindow = lookbackWindow
        cur = self.conn.cursor()
        
        sql = """
              CREATE VIEW TBView AS SELECT Ind.Date,Ind.TC 
              """
        for stk in stockUniverse:
            _sql = ",'{}'.TC_Adj".format(stk)
            sql+=_sql
        _sql = """
               FROM
               (SELECT Date,TC
                      FROM IndexData 
                      WHERE StkCode='{}')Ind
               """.format(marketIndex)
        sql+=_sql
        for stk in stockUniverse:
            _sql = """
                   LEFT JOIN (SELECT TC_Adj,Date 
                              FROM StockData
                              WHERE StkCode='{}')'{}'
                   ON '{}'.Date=Ind.Date
                   """.format(stk,stk,stk)
            sql += _sql
        cur.execute(sql)
        cur.execute("""
                    CREATE TABLE DATA AS SELECT * FROM TBView
                    """)
        cur.execute("""
                    CREATE INDEX _id ON DATA (Date)
                    """)

        
    #----------------------------------------------------------------------
    def Calc(self,date,stock):
        """"""
        tm1 = time.time()
        cur = self.conn.cursor()
        cur.execute("""
                    SELECT *
                    FROM DATA
                    WHERE Date<='{}'
                    ORDER BY Date DESC LIMIT {}
                    """.format(date,self.lookbackWindow+1))
        vals = cur.fetchall()
        tm2 = time.time()
        print tm2-tm1        
        _tc = []
        if len(vals)<self.lookbackWindow+1:
            beta = numpy.nan   
        else:
            for v in vals:
                print v
        #        _tc.append([float(v[1]),float(v[2])])#
        #    tc = numpy.array(_tc)
        #    tce = numpy.log(tc[0::self.returnHorizon])
        #    rets = -numpy.diff(tce,1,0)
        #    sigma = numpy.var(rets)[0,0]
        #print rets      
        #tm2 = time.time()
        #print tm2-tm1            

            
  
            
            
if __name__ == "__main__":
    betaCalc = BetaCalc("MktData\\MktData_Wind_CICC.db", 1,"20100127")
    betaCalc.SetParameters("000300",["600837","600031"],2,250)
    betaCalc.Calc("20160128","600837")
        
        
            
        
    