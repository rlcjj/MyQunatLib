#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<>
  Purpose: wushifan221@gmail.com
  Created: 2016/4/19
"""

import os,sys,logging,time,decimal,codecs
import numpy as np
import pandas as pd
import sqlite3 as lite
import time
from datetime import datetime,timedelta

import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import Tools.Draw as Draw




########################################################################
class ComputeICs(object):
    """
    计算因子IC
    """

    #----------------------------------------------------------------------
    def __init__(self,dbPathZScores,dbPathMarketData,begDate,endDate,conn=None,logger=None):
        """Constructor"""
        if logger == None:
            self.logger = logging.logger()
        else:
            self.logger = logger
        
        dbPathRawData = GetPath.GetLocalDatabasePath()["RawEquity"]
        dbPathProcessedData = GetPath.GetLocalDatabasePath()["ProcEquity"]   
        
        #价格数据录入内存数据库
        self.connDbMkt = lite.connect(":memory:")
        self.connDbMkt.text_factory = str
        cur = self.connDbMkt.cursor()
        self.logger.info("<{}>-Load local database into in-memory database".format(__name__.split('.')[-1]))
        _dbPathMarketData = dbPathRawData + dbPathMarketData 
        cur.execute("ATTACH '{}' AS MktData".format(_dbPathMarketData))
        cur.execute("CREATE TABLE MktData AS SELECT StkCode,Date,TC_Adj FROM MktData.AStockData WHERE Date>='{}'".format(begDate))
        cur.execute("CREATE INDEX mId ON MktData (Date,StkCode)")
        self.logger.info("<{}>-Done".format(__name__.split('.')[-1]))         
        
        #连接因子数据库
        dbFactorValue = dbPathProcessedData + dbPathZScores
        self.connDbFV = lite.connect(dbFactorValue)
        self.connDbFV.text_factory = str
        
        #获取交易日
        self._trdDays = GetTrdDay.GetTradeDays()        
        self.revalueDays = []
        self.trdDays = []
        cur = self.connDbFV.cursor()
        cur.execute("SELECT DISTINCT Date FROM FactorValues ORDER BY Date ASC")
        rows = cur.fetchall()
        for row in rows:
            if row[0]<=endDate and row[0]>=begDate:
                self.revalueDays.append(row[0])
        for d in self._trdDays:
            if d>=self.revalueDays[0] and d<=endDate and d>=begDate:
                self.trdDays.append(d)     
                
                
    #----------------------------------------------------------------------
    def GetFactorNames(self):
        """
        获取数据库中所有的因子名称
        """
        cur = self.connDbFV.cursor()
        cur.execute("PRAGMA table_info(FactorValues)")
        cols = cur.fetchall()
        factors = []
        for col in cols[8:]:
            factors.append(col[1])
        self.factorNames = factors
        factorStr = ""
        for factor in factors:
            factorStr+=(factor+',')
        self.factorStr = factorStr.rstrip(',')
        return factors
        
    
    #----------------------------------------------------------------------
    def GetZScores(self,tbNameZScores,inHS300,date,excludeIndus):
        """
        获取ZScores
        """
        cur = self.connDbFV.cursor()
        cur.execute("""
                    SELECT StkCode,{}
                    FROM {} 
                    WHERE HS300Constituent IN ({}) AND Date='{}' AND ReportType NOT IN ({}) 
                    """.format(self.factorStr,tbNameZScores,inHS300,date,excludeIndus))
        rows = cur.fetchall()
        stockCode = []
        zscores = []
        for row in rows:
            stockCode.append(row[0])
            zscores.append(row[1:])
        _zscores = np.array(zscores,dtype=np.float)
        self.zscores = pd.DataFrame(_zscores,index=stockCode,columns=self.factorNames)
        self.zscoresDay = date
        #self.zscores.to_csv('zscores.csv')
        
        
    #----------------------------------------------------------------------
    def GetStockReturn(self,days1,days2):
        """
        计算股票收益率
        """
        p = self.trdDays.index(self.zscoresDay)
        dateBeg = self.trdDays[p+days1]
        dateEnd = self.trdDays[p+days1+days2]
        print dateEnd,dateBeg
        zscoresStockReturn = []
        cur = self.connDbMkt.cursor()
        sql = "SELECT TC_Adj FROM MktData WHERE StkCode='{}' AND Date in ('{}','{}') ORDER BY Date ASC"
        #f = open('ret.csv','w')
        for stkCode in self.zscores.index:
            cur.execute(sql.format(stkCode,dateBeg,dateEnd))
            rows = cur.fetchall()
            if len(rows)<2:
                _ret = np.nan
            else:
                _ret = np.log(rows[1][0])-np.log(rows[0][0])
            zscoresStockReturn.append(_ret)
            #f.write(stkCode+','+repr(rows[1][0])+','+repr(rows[0][0])+'\n')
        self.zscoresStockReturn = np.array(zscoresStockReturn)
        #f.close()
    
    #----------------------------------------------------------------------
    def ComputeIC(self,factorName):
        """
        计算单个因子给定日期的IC
        """
        mat = pd.DataFrame([self.zscores[factorName].values,self.zscoresStockReturn]).transpose()
        #print mat
        corr = mat.corr().values
        return corr[0,1]
    
    
    #----------------------------------------------------------------------
    def ComputeICsAndSave(self,tbNameZScores,universeIndex,excludeIndus,*returnHorizon):
        """
        计算所有因子的IC并存入数据库
        """
        tbNameICs = "FactorICs_{}".format(universeIndex)
        print tbNameICs
        cur = self.connDbFV.cursor()
        cur.execute("DROP TABLE IF EXISTS {}".format(tbNameICs))
        sqlStr = "Date TEXT,days1 INT,days2 INT"
        for fn in self.factorNames:
            sqlStr += (','+fn+" FLOAT")
        cur.execute("CREATE TABLE {}({})".format(tbNameICs,sqlStr))
        insertSql = "?,?,?"+",?"*len(self.factorNames)
        if universeIndex=="HS300":
            inHS300 = '1'
        else:
            inHS300 = '0'
        for date in self.revalueDays:
            print date
            self.GetZScores(tbNameZScores,inHS300,date,excludeIndus)
            for returnDays in returnHorizon:
                days1 = returnDays[0]
                days2 = returnDays[1]
                self.GetStockReturn(days1,days2)
                _insertRow = [date,days1,days2]
                for fn in self.factorNames:
                    ic = self.ComputeIC(fn)
                    _insertRow.append(ic)
                insertRow = tuple(_insertRow)
                cur.execute("INSERT INTO {} VALUES ({})".format(tbNameICs,insertSql),insertRow)
            self.connDbFV.commit()
            
            
    #----------------------------------------------------------------------
    def ExploreICDecay(self,tbNameICs,begDate,endDate,returnHorizonList):
        """"""
        cur = self.connDbFV.cursor()
        res = pd.DataFrame()
        for days in returnHorizonList:
            sql = "SELECT {} FROM {} WHERE DATE>='{}' AND DATE<='{}' AND Days2={}"
            cur.execute(sql.format(self.factorStr,tbNameICs,begDate,endDate,days))
            rows = cur.fetchall()
            df = pd.DataFrame(rows,columns=self.factorNames)
            m = df.mean(axis=0)
            dfm = m.to_frame(days)
            res = pd.concat([res,dfm],axis=1)
        res.to_csv("IC_Decay.csv")
        
            
            
                
        