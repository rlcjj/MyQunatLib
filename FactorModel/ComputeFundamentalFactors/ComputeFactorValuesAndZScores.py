#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/25
"""

import os,sys,logging ,time,decimal,codecs,numpy,re
import sqlite3 as lite
from datetime import datetime,timedelta
from ConfigParser import ConfigParser

root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import FactorModels.ComputeFundamentalFactors._CalculateFactorValues as CalcFactorVals
import InvestmentUniverse.GetIndexConstituentStocks as GetIndexConstituentStocks
import Tools.LogOutputHandler as LogHandler


########################################################################
class ComputeFactorsAndZScores(object):
    """
    计算给定投资空间中的所有股票
    基本面因子值并在横截面做标准化
    处理转换成ZScores
    """
    
    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("ComputeFactorsAndZScores")
        else:    
            self.logger = logger
            
        dbPathProcessedData = GetPath.GetLocalDatabasePath()["ProcEquity"]
        self.dbPathProcessedData = dbPathProcessedData
        self.totalTradeDay = GetTrdDay.GetTradeDays()   
        

    #----------------------------------------------------------------------
    def LoadSourceData(self,dbPathFdmtData,dbPathMktData,dbPathConstituentStocks):
        """"""
        self.indexCompStks = GetIndexConstituentStocks.GetIndexConstituentStocks(dbPathConstituentStocks,self.logger) 
        self.objCalcFactorVals = CalcFactorVals.CalculateFactorValues(dbPathFdmtData,dbPathMktData)
        
        
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
        self.conn = lite.connect(self.dbPathProcessedData+self.factorDatabaseName+".db")
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
            stkUniver = self.indexCompStks.GetStocks(dt,self.stkUniver)
            hs300 = self.indexCompStks.GetStocks(dt,"000300")
            #zz300 = self.indexCompStks.GetStocks(dt,"000905")
            for stk in stkUniver:
                if stk in hs300:
                    inHS300 = 1
                else:
                    inHS300 = 0                    
                vals = self.objCalcFactorVals.Calc(dt,180,stk,self.factorAlgos)
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
        conn = lite.connect(self.dbPathProcessedData+self.fctDbName+".db")
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

        