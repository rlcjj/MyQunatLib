#!/usr/bin/env python
#coding:utf-8
"""
  Author:   Wusf--<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/15
"""

import os,sys,logging ,time,decimal,codecs
from ConfigParser import ConfigParser
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)

import os,sys,logging
from ConfigParser import ConfigParser
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)
import SynchronizeDatabase.SyncDb as Sync

########################################################################
class SyncData(Sync.SyncDb):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logOutputHandler):
        """Constructor"""
        super(self.__class__,self).__init__("DbInfo.cfg",__name__,logOutputHandler)


    #----------------------------------------------------------------------
    def ConnRmtDb(self):
        """"""
        super(self.__class__,self).ConnRmtDb("CICCWind","oracle")

        
    #----------------------------------------------------------------------
    def _CreateLocalTable(self):
        """"""
        self.logger.info("Create local table: TradeDate")
        itemStr = ""
        sql = "CREATE TABLE TradeDate(TrdDate TEXT)"
        cur = self.locConn.cursor()
        cur.execute("DROP TABLE IF EXISTS TradeDate")
        cur.execute(sql)          
        
        
    #----------------------------------------------------------------------
    def Sync(self):
        """"""
        self._CreateLocalTable()
        self.logger.info("  Update table:TradeDate")
        curR = self.conn.cursor()
        curW = self.locConn.cursor()
        curW.execute("PRAGMA synchronous = OFF")
        itemStr = ""
        try:
            curR.execute("SELECT F1_1010 FROM WINDDB.TB_OBJECT_1010")
        except Exception,e:
            self.logger.error(e)
        rows = curR.fetchall()
        for row in rows:
            curW.execute("INSERT OR IGNORE INTO TradeDate VALUES (?)",row)
        self.locConn.commit()        
    
        
        
    
    