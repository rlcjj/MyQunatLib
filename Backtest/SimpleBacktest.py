#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/2
"""

import os,sys
import sqlite3 as lite
import numpy as np
import Tools.GetLocalDatabasePath as GetPath
import Tools.GetTradeDays as GetTrdDay
import Tools.Draw as Draw


########################################################################
class SimpleBacktest(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logger):
        """Constructor"""
        self.logger = logger
        self.logger.info("<{}>-Initiate object".format(__name__.split('.')[-1]))
        
        
    #----------------------------------------------------------------------
    def LoadDatabase(self,dbNameMarketData,startDate,useInMemory,conn):
        """"""
        self.logger.info("<{}>-Load local market database".format(__name__.split('.')[-1]))
        dbPath = GetPath.GetLocalDatabasePath()["EquityDataRaw"]
        dbPath = dbPath + dbNameMarketData
        if conn!=None:
            self.conn = conn
        else:
            if useInMemory == 1:
                self.conn = lite.connect(":memory:")
                self.conn.text_factory = str
                cur = self.conn.cursor()
                cur.execute("ATTACH '{}' AS MarketData".format(dbPath))
                cur.execute("CREATE TABLE AStockData AS SELECT StkCode,Date,LC,TC,Vol FROM MarketData.AStockData WHERE Date>'{}'".format(startDate))
                cur.execute("CREATE INDEX idS ON AStockData(Date,StkCode)")
                cur.execute("CREATE TABLE IndexData AS SELECT StkCode,Date,LC,TC,Vol FROM MarketData.IndexData WHERE Date>'{}'".format(startDate))
                cur.execute("CREATE INDEX idI ON IndexData(Date,StkCode)")  
            else:
                self.conn = lite.connect(dbPath)
                self.conn.text_factory = str
                
                
    #----------------------------------------------------------------------
    def LoadPortfolios(self,portfolios):
        """"""
        self.logger.info("<{}>-Load portfolios".format(__name__.split('.')[-1]))
        self.trdDate = GetTrdDay.GetTradeDays()
        self.rebalanceDate = sorted(portfolios.keys())
        self.ports = portfolios
    
    
    #----------------------------------------------------------------------
    def _GetStocksAverageReturn(self,date,stockList):
        """"""
        self.logger.info("<{}>-Calculate portfolio return-{}".format(__name__.split('.')[-1],date))
        cur = self.conn.cursor()
        if len(stockList)==0:
            rets = {"None":[0]}
        else:
            rets = {}
            for stk in stockList:
                sql = """
                      SELECT (TC-LC)/LC
                      FROM AStockData
                      WHERE Date='{}'
                      AND StkCode='{}'
                      """
                cur.execute(sql.format(date,stk))
                content = cur.fetchone()
                #if date=="20141231":
                #    print stk,content
                if content!=None and content[0]!=None:
                    rets[stk]=(content[0])
                else:
                    rets[stk] = np.nan
        return rets
        
        
    #----------------------------------------------------------------------
    def _GetIndexReturn(self,date,indexCode):
        """"""
        self.logger.info("<{}>-Calculate index {} return-{}".format(__name__.split('.')[-1],indexCode,date))
        cur = self.conn.cursor()
        sql = """
              SELECT (TC-LC)/LC
              FROM IndexData
              WHERE Date='{}'
              AND StkCode='{}'
              """
        cur.execute(sql.format(date,indexCode))
        content = cur.fetchone()
        return content[0]
        
        
    #----------------------------------------------------------------------
    def Run(self,strategyName,begDate,endDate,benchMark):
        """"""
        self.logger.info("<{}>-Start backtesting".format(__name__.split('.')[-1]))
        self.strategyName = strategyName
        trdDate = []
        for d in self.trdDate:
            if d>=begDate and d<=endDate:
                trdDate.append(d)
        self._trdDate =trdDate
        rebDate = self.rebalanceDate
        
        longPort = []
        shortPort = []
        longRet = 0
        shortRet = 0
        hedgedRet = 0
        indexRet = 0
        self.longRetList = []
        self.shortRetList = []
        self.hedgedRetList = []
        self.indexRetList = []
        
        self.debugInfo = {"date":[],"long_rets":[],"short_rets":[]}
        for d in trdDate:

            longRetsDict = self._GetStocksAverageReturn(d,longPort)
            shortRetsDict = self._GetStocksAverageReturn(d,shortPort)
            longRet = np.nanmean(longRetsDict.values())
            shortRet = np.nanmean(shortRetsDict.values())
            indexRet = self._GetIndexReturn(d,benchMark)
            hedgedRet = longRet-shortRet
            self.longRetList.append(longRet)
            self.shortRetList.append(shortRet)
            self.hedgedRetList.append(hedgedRet)
            self.indexRetList.append(indexRet)
            
            if d in rebDate:
                longPort = self.ports[d]["long"]
                shortPort = self.ports[d]["short"]
            
            self.debugInfo["long_rets"].append(longRetsDict)
            self.debugInfo["short_rets"].append(shortRetsDict)                
            self.debugInfo["date"].append(d)
            
    #----------------------------------------------------------------------
    def Output(self,dirName,debug=0):
        """"""
        self.logger.info("<{}>-Output results".format(__name__.split('.')[-1]))
        if not os.path.exists(dirName):
            os.mkdir(dirName)
        res = open(dirName+self.strategyName+".csv",'w')
        res.write("date,long_ret,short_ret,hedged_ret,index_ret\n")
        for i in xrange(len(self._trdDate)):
            res.write("{},{},{},{},{}\n".format(self._trdDate[i],self.longRetList[i],self.shortRetList[i],self.hedgedRetList[i],self.indexRetList[i]))
        res.close()
        
        if debug==1:
            dbg = open("{}\\debug_".format(dirName)+self.strategyName+".csv",'w')
            for i in xrange(len(self.debugInfo["date"])):
                dbg.write(self.debugInfo["date"][i])
                for item in self.debugInfo["long_rets"][i].keys():
                    dbg.write(','+item)
                dbg.write("\n")
                for item in self.debugInfo["long_rets"][i].keys():
                    dbg.write(','+repr(self.debugInfo["long_rets"][i][item]))
                dbg.write("\n")                
                for item in self.debugInfo["short_rets"][i].keys():
                    dbg.write(','+item)
                dbg.write("\n")  
                for item in self.debugInfo["short_rets"][i].keys():
                    dbg.write(','+repr(self.debugInfo["short_rets"][i][item]))
                dbg.write("\n")  
            dbg.close()
        
        Draw.DrawCumulativeReturnCurve(self._trdDate,
                                       {"HedgedReturn":self.hedgedRetList},
                                        [{"IndexReturn":self.indexRetList},{"LongPortReturn":self.longRetList},{"ShortPortReturn":self.shortRetList}],
                                       dirName,self.strategyName)
        
        
            
            
        
            
                
                    
            
        
        
    
    