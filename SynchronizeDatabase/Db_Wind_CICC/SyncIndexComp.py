#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/14
"""

import os,sys,logging,time
from ConfigParser import ConfigParser
import Configs.RootPath as Root
RootPath = Root.RootPath
import SynchronizeDatabase.SyncDb as Sync

########################################################################
class SyncData(Sync.SyncDb):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,logOutputHandler):
        """Constructor"""
        super(self.__class__,self).__init__("DatabaseInfo.cfg",__name__,logOutputHandler)


    #----------------------------------------------------------------------
    def ConnRmtDb(self):
        """"""
        super(self.__class__,self).ConnRmtDb("CICCWind","oracle")

        
    #----------------------------------------------------------------------
    def _CreateLocalTable(self,tb):
        """"""
        self.logger.info("Create local table: %s"%tb[1])
        itemStr = ""
        for item in self.tableItems[tb[0]]:
            _strAdd = ","+item[1].split('|')[0]+" "+item[1].split('|')[1]
            itemStr += _strAdd
        sql = self.tbCfg.get("SQL:"+tb[0],"Create").format(tb[1],itemStr)
        cur = self.locConn.cursor()
        cur.execute(sql)          
        
        
    #----------------------------------------------------------------------
    def _UpdateLocalTable(self,tb):
        """"""
        self.logger.info("  Update table:{}".format(tb[0]))
        fSql = self.tbCfg.get("SQL:"+tb[0],"Fetch")
        #iSql = self.tbCfg.get("SQL:"+tb[0],"Insert")
        curR = self.conn.cursor()
        curW = self.locConn.cursor()
        curW.execute("PRAGMA synchronous = OFF")
        itemStr = ""
        for item in self.tableItems[tb[0]]:
            _strAdd = ","+item[0]
            itemStr += _strAdd 
        try:
            curR.execute(fSql.format(itemStr))
        except Exception,e:
            print fSql.format(itemStr)
            self.logger.error(e)
        rows = curR.fetchall()
        #print fSql.format(itemStr,startDate)
        for row in rows:
            #print "INSERT OR IGNORE INTO {} VALUES {})".format(tb[0],str(row))
            curW.execute("INSERT OR IGNORE INTO {} VALUES (?{})".format(tb[1],',?'*(len(row)-1))
                        ,row)
        self.locConn.commit()
        curW.execute("Create Index {} On {}(StkCode)".format("Ind"+tb[1],tb[1]))
        self.locConn.commit()
    
        
        
    
    