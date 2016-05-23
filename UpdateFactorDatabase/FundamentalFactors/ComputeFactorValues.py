#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/25
"""

import os,numpy
import sqlite3 as lite
from ConfigParser import ConfigParser

import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import UpdateFactorDatabase.FundamentalFactors._CalculateFactorValues as CalcFactorVals
import InvestmentUniverse.GetIndexConstituentStocks as GetIndexConstituentStocks
import Tools.LogOutputHandler as LogHandler
import Configs.RootPath as Root
RootPath = Root.RootPath 


########################################################################
class ComputeFactorValues(object):
    """
    计算给定投资空间中的所有股票
    基本面因子值
    """
    
    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        #Create log file
        if logger == None:
            self.logger = LogHandler.LogOutputHandler("ComputeFactorsAndZScores")
        else:    
            self.logger = logger
            
        dbPathProcessedData = GetPath.GetLocalDatabasePath()["EquityDataRefined"]
        self.dbPathProcessedData = dbPathProcessedData
        self.totalTradeDay = GetTrdDay.GetTradeDays()   
        

    #----------------------------------------------------------------------
    def LoadSourceData(self,dbPathFdmtData,dbPathMktData,dbPathConstituentStocks):
        """
        读取本地数据库数据
        """
        self.objConstituentStocks = GetIndexConstituentStocks.GetIndexConstituentStocks(dbPathConstituentStocks,self.logger) 
        self.objCalcFactorVals = CalcFactorVals.CalculateFactorValues(dbPathFdmtData,dbPathMktData,None,self.logger)
        
    
    #----------------------------------------------------------------------
    def LoadFactorAlgos(self,factorStyle):
        """
        Load algorithem for computing factor values
        """
        self.factorNames = []
        self.factorAlgos = []
        for style in factorStyle:
            path = RootPath+"\\UpdateFactorDatabase\\FundamentalFactors\\FactorAlgos\\"+style
            algoFiles = os.listdir(path)
            for algoFile in algoFiles: 
                algoName = algoFile.split('.')
                if algoName[0][0]!='_' and algoName[1]=="py":
                    self.logger.info("<{}>-Load factor algo {} {}".format(__name__.split('.')[-1],style,algoName[0]))
                    self.factorNames.append(algoName[0])
                    exec("import UpdateFactorDatabase.FundamentalFactors.FactorAlgos.{}.{} as algo".format(style,algoName[0]))
                    self.factorAlgos.append(algo)
                    
    
    #----------------------------------------------------------------------
    def ComputeAndSaveFactorValues(self,factorDatabaseName,begDate):
        """
        Start to run factor computation
        """
        self.conn = lite.connect(self.dbPathProcessedData+factorDatabaseName+".db")
        self.conn.text_factory = str
        self.cur = self.conn.cursor()
        self.cur.execute("PRAGMA synchronous = OFF")
        self.cur.execute("DROP TABLE IF EXISTS FundamentalFactors")
        sqlStr = ""
        for item in self.factorNames:
            sqlStr+=','+item+" FLOAT"
        self.cur.execute("""
                         CREATE TABLE FundamentalFactors(StkCode TEXT,
                                                 StkName TEXT,
                                                 IndusCode TEXT,
                                                 IndusName TEXT,
                                                 Date TEXT,
                                                 AcctPeriod TEXT,
                                                 ReportType TEXT
                                                 {})
                         """.format(sqlStr))   
        
        insertSql = "?,?,?,?,?,?,?"+len(self.factorAlgos)*",?"
        
        allStkCodes = self.objCalcFactorVals.GetAllStockCodes()
         
        for stk in allStkCodes:
            self.logger.info("<{}>-Compute factor of {}".format(__name__.split('.')[-1],stk))
            declareDate = self.objCalcFactorVals.GetFundamentalDataDeclareDate(stk,begDate)

            for dt in declareDate:                  
                vals = self.objCalcFactorVals.Calculate(dt,180,stk,self.factorAlgos)
                stkInfo = self.objConstituentStocks.GetStockNameAndIndustry(stk,dt)
                if vals!=None and vals[-1]!=None:
                    if stkInfo!=None:
                        row = [stk,stkInfo[0],stkInfo[1],stkInfo[2],dt,vals[0],vals[1]]
                    else:
                        row = [stk,None,None,None,dt,vals[0],vals[1]]
                    for val in vals[2]:
                        row.append(val)
                    self.cur.execute("INSERT INTO FundamentalFactors VALUES ({})".format(insertSql),tuple(row))
        self.conn.commit()
        self.cur.execute("CREATE INDEX Id ON FundamentalFactors(Date,StkCode)")
        self.conn.commit()
        
        
    #----------------------------------------------------------------------
    def ComputeAndSaveZScores(self,configPath,classification):
        """
        因子值标准化
        """
        conf = ConfigParser()
        conf.read(configPath)
        self.logger.info("<{}>-Load industry configs".format(__name__.split('.')[-1]))
        indusList = conf.items(classification)
        conn = lite.connect(self.dbPathProcessedData+self.factorDatabaseName+".db")
        conn.text_factory = str
        cur = conn.cursor()
        
        cur.execute("PRAGMA table_info(FactorValues)")
        cols = cur.fetchall()
        sqlStr=cols[0][1]+' '+cols[0][2]
        for t in cols[1:]:
            sqlStr+=','+t[1]+" "+t[2]
        cur.execute("DROP TABLE IF EXISTS ZScores")
        cur.execute("CREATE TABLE ZScores({})".format(sqlStr)) 
        insertSql = "?"+",?"*(len(cols)-1)
        
        cur.execute("SELECT DISTINCT Date FROM FactorValues ORDER BY Date")
        dates = cur.fetchall()
        for dt in dates:
            date = dt[0]
            i=0
            for indus in indusList:
                self.logger.info("<{}>-Process industy {}, {}".format(__name__.split('.')[-1],indus[0],dt))
                cur.execute("SELECT * FROM FactorValues WHERE Date='{}' AND IndusCode in ({})".format(date,indus[0]))
                #print "SELECT StkCode FROM FactorValues WHERE Date='{}' AND IndusCode in ({})".format(date,indus[0])
                rows = cur.fetchall()
                if len(rows)>0:
                    i+=1
                    _mat = []
                    stkInfo = []
                    for row in rows:
                        stkInfo.append(row[0:8])
                        _mat.append(row[8:])
                    mat = numpy.array(_mat,dtype=numpy.float)

                    w_mat = self.Winsorize(mat, 3)                   
                    for k in xrange(len(stkInfo)):
                        r = list(stkInfo[k])+w_mat[k,:].tolist()
                        cur.execute("INSERT INTO ZScores VALUES ({})".format(insertSql),tuple(r))
                    #print i,indus,dt          
        conn.commit()
        cur.execute("CREATE INDEX Id2 ON ZScores(Date,StkCode)")
        conn.commit()        


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

        