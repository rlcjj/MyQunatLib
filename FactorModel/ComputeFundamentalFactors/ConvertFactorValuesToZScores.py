#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/4/12
"""

import os,sys,logging ,time,decimal,codecs,numpy,re
import sqlite3 as lite
from datetime import datetime,timedelta
from ConfigParser import ConfigParser

root = os.path.abspath("D:\\MyQuantLib\\")
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import FactorModel.ComputeFundamentalFactors._CalculateFactorValues as CalcFactorVals
import InvestmentUniverse.GetIndexConstituentStocks as GetIndexConstituentStocks
import Tools.LogOutputHandler as LogHandler
import Tools.WinzAndTrim as modWinzAndTrim


########################################################################
class ConvertFactorValuesToZScores(object):
    """
    把基本面因子原始值转换为ZScores
    """

    #----------------------------------------------------------------------
    def __init__(self,logger=None):
        """Constructor"""
        #Create log file
        if logger == None:
            self.logger = logging.Logger("")
        else:
            self.logger = logger
            
    
    #----------------------------------------------------------------------
    def ConnectToFactorDatabase(self,dbNameFactorValues):
        """
        读取基本面因子数据库
        """
        self.logger.info("<{}>-Connect to local factor database".format(__name__.split('.')[-1]))
        dbPathProcessedData = GetPath.GetLocalDatabasePath()["ProcEquity"]        
        self.conn = lite.connect(dbPathProcessedData+dbNameFactorValues)
        self.conn.text_factory = str
        self.logger.info("<{}>-Create in-memory database for factor values".format(__name__.split('.')[-1]))
        self.connIM = lite.connect(":memory:")
        self.connIM.text_factory = str
        curIM = self.connIM.cursor()
        curIM.execute("ATTACH '{}' AS FactorVals".format(dbPathProcessedData+dbNameFactorValues))
        
        
    #----------------------------------------------------------------------
    def GetFactorNames(self,fileNameOutlierCFG):
        """
        获取数据库中的基本面因子名
        """
        cur = self.conn.cursor()
        cur.execute("PRAGMA TABLE_INFO(FactorValues)")
        cols = cur.fetchall()
        f = open(fileNameOutlierCFG,'w')
        for col in cols[8:]:
            f.write(col[1]+':'+"\n")
        f.close()
        
        
    #----------------------------------------------------------------------
    def LoadOutlierRuleConfig(self,fileNameOutlierCFG):
        """
        读取处理Outlier的配置文件
        """
        self.outlierRule = {}
        f = open(fileNameOutlierCFG,'r')
        for line in f:
            content = line.rstrip().split(':')
            self.outlierRule[content[0]] = content[1]
        factorWinz = ""
        factorTrim = ""
        for k in self.outlierRule.keys():
            if self.outlierRule[k] == 'T':
                factorTrim+=(k+',')
            else:
                factorWinz+=(k+',')
        self.factorTrim = factorTrim.rstrip(',')
        self.factorWinz = factorWinz.rstrip(',')
        
            
            
    #----------------------------------------------------------------------
    def LoadFactorValues(self):
        """
        Load factor value database into in-memory database
        """
        self.logger.info("<{}>-Load factor values into in-memory database...".format(__name__.split('.')[-1]))
        curIM = self.connIM.cursor()
        curIM.execute("CREATE TABLE FactorValues AS SELECT * FROM FactorVals.FactorValues")
        curIM.execute("CREATE INDEX factorID ON FactorValues (StkCode,Date)")
        self.logger.info("<{}>-Done!".format(__name__.split('.')[-1]))
    
           
    #----------------------------------------------------------------------
    def ToZScores(self,cutoff1,cutoff2):
        """"""
        curL = self.conn.cursor()
        curI = self.connIM.cursor()
        
        self.logger.info("<{}>-Create local zscore database...".format(__name__.split('.')[-1]))
        curL.execute("PRAGMA table_info(FactorValues)")
        cols = curL.fetchall()    
        sqlStr=cols[0][1]+' '+cols[0][2]
        for t in cols[1:8]:
            sqlStr+=','+t[1]+" "+t[2]  
        for f in self.factorTrim.split(','):
            sqlStr+=','+f+' '+"FLOAT"
        for f in self.factorWinz.split(','):
            sqlStr+=','+f+' '+"FLOAT"
        curL.execute("DROP TABLE IF EXISTS ZScores")
        curL.execute("CREATE TABLE ZScores({})".format(sqlStr)) 
        insertSql = "?"+",?"*(len(cols)-1)   
        self.logger.info("<{}>-Done!".format(__name__.split('.')[-1]))
        
        curI.execute("SELECT DISTINCT Date FROM FactorValues ORDER BY Date")
        date = curI.fetchall()
        for _dt in date:
            dt = _dt[0]
            self.logger.info("<{}>-Convert factor to zscore at {}!".format(__name__.split('.')[-1],dt))
            #These factors' outlier should be trimed
            curI.execute("SELECT {} FROM FactorValues WHERE Date={}".format(self.factorTrim,dt))
            rows = curI.fetchall()
            mat = numpy.array(rows,dtype=numpy.float)
            matT = modWinzAndTrim.Trim(mat,cutoff1)
            #These factors' outlier should be winsorized
            curI.execute("SELECT {} FROM FactorValues WHERE Date={}".format(self.factorWinz,dt))
            rows = curI.fetchall()
            mat = numpy.array(rows,dtype=numpy.float)
            matW = modWinzAndTrim.Wins(mat,cutoff1)            
            matN = numpy.hstack((matT,matW))
            score = modWinzAndTrim.Standardize(matN,cutoff2)
            #Stock infomation
            curI.execute("SELECT StkCode,StkName,IndusCode,IndusName,Date,AcctPeriod,ReportType,HS300Constituent FROM FactorValues WHERE Date={}".format(dt))
            rows = curI.fetchall()
            for r in xrange(len(rows)):
                val = list(rows[r])+score[r].tolist()
                curL.execute("INSERT INTO ZScores VALUES ({})".format(insertSql),tuple(val))
        self.conn.commit()
        
        
    #----------------------------------------------------------------------
    def NeutralizeZScores(self,subdivision):
        """
        在行业内对ZScores做中性处理
        """
        print "To be continued..."
        
            
            
