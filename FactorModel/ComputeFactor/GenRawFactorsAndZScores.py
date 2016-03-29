#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/25
"""

import os,sys,logging ,time,decimal,codecs,numpy,re
import sqlite3 as lite
import time
from datetime import datetime,timedelta
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import Tools.Draw as Draw
import FactorAlgos.CalcFactorVals as CalcFactorVals
import DefineInvestUniverse.GetIndexCompStocks as IndexCompStks
from ConfigParser import ConfigParser

########################################################################
class ComputeFactorsAndZScores(object):
    """"""
    
    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("ComputeFactorsAndZScores")
        else:    
            self.logger = logger
            
        localProcDataDBPath = GetPath.GetLocalDatabasePath()["ProcEquity"]
        self.procDbPath = localProcDataDBPath
        self.totalTradeDay = GetTrdDay.GetTradeDays()   

    #----------------------------------------------------------------------
    def LoadSourceData(self,finRptDataDbAddr,mktDataDbAddr,indexCompStksDataDbAddr):
        """"""
        self.indexCompStks = IndexCompStks.GetIndexCompStocks(indexCompStksDataDbAddr) 
        self.calcFactorVals = CalcFactorVals.CalcFactorVals(finRptDataDbAddr,mktDataDbAddr)
        
    #----------------------------------------------------------------------
    def SetStockUniverseAndFactorReCalcDate(self,stockUnviverIndex,begDate,endDate,holdingPeriod):
        """
        设定股票的投资范围和因子估算的日期
        """
        self.tradeDays = []
        self.reCalcDayMark = {}
        self.reCalcDate = []
        k = 0
        for dt in self.totalTradeDay:
            if dt>=begDate and dt<=endDate:
                self.tradeDays.append(dt)
                k+=1
                if k % holdingPeriod == 1:
                    self.reCalcDayMark[dt]=1
                    self.reCalcDate.append(dt)
                else:
                    self.reCalcDayMark[dt]=0  
        self.stkUniver = stockUnviverIndex
        self.factorDatabaseName = "Factor_"+stockUnviverIndex+"_"+repr(holdingPeriod)+"D"
        
    
    #----------------------------------------------------------------------
    def LoadFactorAlgos(self,*factorStyle):
        """"""
        self.factorNames = []
        self.factorAlgos = []
        for style in factorStyle:
            path = root + "\\FactorModel\\ComputeFactor\\FactorAlgo\\FactorAlgos\\"+style
            algoFiles = os.listdir(path)
            for algoFile in algoFiles: 
                algoName = algoFile.split('.')
                if algoName[0][0]!='_' and algoName[1]=="py":
                    self.logger.info("Load factor algo {}".format(algoName[0]))
                    self.factorNames.append(algoName[0])
                    exec("import FactorAlgos.{}.{} as algo".format(style,algoName[0]))
                    self.factorAlgos.append(algo)
                    
    
    #----------------------------------------------------------------------
    def ComputeFactors(self):
        """"""
        self.conn = lite.connect(self.procDbPath+self.fctDbName+".db")
        self.conn.text_factory = str
        self.cur = self.conn.cursor()
        self.cur.execute("PRAGMA synchronous = OFF")
        self.cur.execute("DROP TABLE IF EXISTS FactorVals")
        sqlStr = ""
        for item in self.factorNames:
            sqlStr+=','+item+" FLOAT"
        self.cur.execute("""
                    CREATE TABLE FactorVals(StkCode TEXT,
                                            StkName TEXT,
                                            IndusCode TEXT,
                                            IndusName TEXT,
                                            Date TEXT,
                                            AcctPeriod TEXT,
                                            ReportType TEXT,
                                            InHS300 INT
                                            {})
                    """.format(sqlStr))   
        
        insertSql = "?,?,?,?,?,?,?,?"+len(self.factorAlgos)*",?"
        for dt in self.revalueDate:
            tm1 = time.time()
            stkUniver = self.indexCompStks.GetStocks(dt,self.stkUniver)
            hs300 = self.indexCompStks.GetStocks(dt,"000300")
            #zz300 = self.indexCompStks.GetStocks(dt,"000905")
            for stk in stkUniver:
                if stk in hs300:
                    inHS300 = 1
                else:
                    inHS300 = 0                    
                vals = self.calcFactorVals.Calc(dt,180,stk,self.factorAlgos)
                stkInfo = self.indexCompStks.GetStockNameAndIndustry(stk,dt)
                #print stk,stkInfo[0].decode('utf-8'),stkInfo[1],stkInfo[2]
                if vals!=None and vals[-1]!=None:
                    if stkInfo!=None:
                        row = [stk,stkInfo[0],stkInfo[1],stkInfo[2],dt,vals[0],vals[1],inHS300]
                    else:
                        row = [stk,None,None,None,dt,vals[0],vals[1],inHS300]
                    for val in vals[2]:
                        row.append(val)
                    self.cur.execute("INSERT INTO FactorVals VALUES ({})".format(insertSql),tuple(row))
            tm2 = time.time()
            print dt,tm2-tm1
        self.conn.commit()
        self.cur.execute("CREATE INDEX Id ON FactorVals(Date,StkCode)")
        self.conn.commit()
        
        
        
    #----------------------------------------------------------------------
    def RawFactors2ZScores(self,stockUniverse,configPath,classification):
        """"""
        tm1 = time.time()
        conf = ConfigParser()
        conf.read(configPath)
        print conf.sections()
        indusList = conf.items(classification)
        conn = lite.connect(self.procDbPath+self.fctDbName+".db")
        conn.text_factory = str
        cur = conn.cursor()
        
        cur.execute("PRAGMA table_info(FactorVals)")
        cols = cur.fetchall()
        print cols
        sqlStr=cols[0][1]+' '+cols[0][2]
        for t in cols[1:]:
            sqlStr+=','+t[1]+" "+t[2]
        cur.execute("DROP TABLE IF EXISTS ZScores")
        cur.execute("CREATE TABLE ZScores({})".format(sqlStr)) 
        insertSql = "?"+",?"*(len(cols)-1)
        
        cur.execute("SELECT DISTINCT Date FROM FactorVals ORDER BY Date")
        dates = cur.fetchall()
        for dt in dates:
            date = dt[0]
            i=0
            for indus in indusList:
                cur.execute("SELECT * FROM FactorVals WHERE Date='{}' AND IndusCode in ({})".format(date,indus[0]))
                #print "SELECT StkCode FROM FactorVals WHERE Date='{}' AND IndusCode in ({})".format(date,indus[0])
                rows = cur.fetchall()
                if len(rows)>0:
                    i+=1
                    _mat = []
                    stkInfo = []
                    for row in rows:
                        stkInfo.append(row[0:8])
                        _mat.append(row[8:])
                    mat = numpy.array(_mat,dtype=numpy.float)
                    #print date,indus[0]

                    w_mat = self.Winsorize(mat, 3)                   
                    for k in xrange(len(stkInfo)):
                        r = list(stkInfo[k])+w_mat[k,:].tolist()
                        cur.execute("INSERT INTO ZScores VALUES ({})".format(insertSql),tuple(r))
                    print i,indus,dt          
        conn.commit()
        cur.execute("CREATE INDEX Id2 ON ZScores(Date,StkCode)")
        conn.commit()        
        tm2 = time.time()
        print tm2-tm1
            #for row in rows:
            #    print len(row)
    #----------------------------------------------------------------------
    def Winsorize(self,mat,std):
        """"""
        _std = numpy.nanstd(mat,0)
        #_std[_std==0]=99999999
        _mean = numpy.nanmean(mat,0)
        _mat = (mat-_mean)/_std
        _mat[_mat>std] = std
        _mat[_mat<-std] = -std
        return _mat

        
        
            
        
        

if __name__ == "__main__":
    dbPath1 = "FumdamentalDerivData.db"
    dbPath2 = "MktData\\MktData_Wind_CICC.db"   
    dbPath3 = "MktGenInfo\\IndexComp_Wind_CICC.db"    
    GFctZScore = GenRawFactorsAndZScoresDatabase()
    GFctZScore.LoadSourceData(dbPath1,dbPath2,dbPath3)
    GFctZScore.SetStockUniverseAndRevalueDate("399906","20070101","20151231",10)
    GFctZScore.LoadFactorAlgos("FdmtFactors.Growth","FdmtFactors.Profitability","FdmtFactors.Quality","FdmtFactors.Value","TechnicalFactors")
    GFctZScore.GenRawFactors()
    #GFctZScore.RawFactors2ZScores("000300","Industry.cfg","SW1_ZZ500")