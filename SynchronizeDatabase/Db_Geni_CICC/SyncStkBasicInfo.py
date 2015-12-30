#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<>
  Purpose: Get stock listing & delisting date
           获取股票基本信息
  Created: 2015/10/30
"""

import os,sys
from ConfigParser import ConfigParser
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)

import SynchronizeDatabase.SyncDb as Sync
import sqlite3 as lite
import chardet

########################################################################
class SyncStkBasicInfo(Sync.SyncDb):
    """
    step1: 
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        print "Get stock listing & delisting date\n"
        super(self.__class__,self).__init__("DbInfo.cfg")
        
    #----------------------------------------------------------------------
    def ConnRmtDb(self):
        """"""
        super(self.__class__,self).ConnRmtDb("CICCGenius","mssql")        
        
    #----------------------------------------------------------------------
    def _UpdateLocalTable(self,tb):
        """"""
        fSql = self.tbCfg.get("SQL:"+tb[0],"Fetch")
        iSql = self.tbCfg.get("SQL:"+tb[0],"Insert")
        curR = self.conn.cursor()
        curR.execute(fSql)
        curW = self.locConn.cursor()
        rows = curR.fetchall()
        for row in rows:
            _row = []
            for i in xrange(len(row)):
                if i==1 or i==4 and row[i]!=None:
                    _row.append(row[i].decode("gbk"))
                else:
                    _row.append(row[i])
            curW.execute(str(iSql)%tb[1],_row)
        self.locConn.commit()
            
        
        

        
        
        
        
    
    