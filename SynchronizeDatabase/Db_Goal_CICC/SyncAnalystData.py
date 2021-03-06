#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wushifan --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/11/13
"""

import os,sys,logging
from ConfigParser import ConfigParser
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
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
        super(self.__class__,self).ConnRmtDb("CICCGoGoal","mssql")
    
    #----------------------------------------------------------------------
    def _CheckLocalTable(self,tb):
        """"""
        super(self.__class__,self)._CheckLocalTable(tb)
        cur = self.locConn.cursor()
        sql = "SELECT DeclareDate FROM "
        cur.execute("""
                    SELECT DeclareDate
                    FROM %s 
                    ORDER BY DeclareDate DESC LIMIT 1"""%tb[1])
        row = cur.fetchone()
        if row!=None:
            self.lastRecDate[tb[0]]=row[0]
        else:
            self.lastRecDate[tb[0]]=self.startDate
        
        
    #----------------------------------------------------------------------
    def _CreateLocalTable(self,tb):
        """
        Create local data tables
        """
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
        if self.replace==1 or len(self.lastRecDate)==0:
            startDate = self.startDate
        else:
            startDate = self.lastRecDate[tb[0]]
        monthRange = []
        for item in self.monthRange:
            if item[0]<=startDate and item[1]>=startDate:
                monthRange.append((startDate,item[1]))
            elif item[0]>=startDate and item[1]>startDate:
                monthRange.append((item[0],item[1]))
              
        fSql = self.tbCfg.get("SQL:"+tb[0],"Fetch")
        #iSql = self.tbCfg.get("SQL:"+tb[0],"Insert")
        curR = self.conn.cursor()
        curW = self.locConn.cursor()
        curW.execute("PRAGMA synchronous = OFF")
        itemStr = ""
        for item in self.tableItems[tb[0]]:
            _strAdd = ","+item[0]
            itemStr += _strAdd 
        for mR in monthRange:
            self.logger.info("    From {} to {}".format(mR[0],mR[1]))
            try:
                curR.execute(fSql.format(itemStr,mR[0],mR[1]))
                #print fSql.format(itemStr,mR[0],mR[1])
            except Exception,e:
                print fSql.format(itemStr,mR[0],mR[1])
                self.logger.error(e)
            rows = curR.fetchall()
            #print fSql.format(itemStr,mR[0],mR[1])
            for row in rows:
                #print "INSERT OR IGNORE INTO {} VALUES {})".format(tb[0],str(row))
                curW.execute("INSERT OR IGNORE INTO {} VALUES (?{})".format(tb[1],',?'*(len(row)-1))
                             ,row)
        self.locConn.commit()
            

            
        
       
        
