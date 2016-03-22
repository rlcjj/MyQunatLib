#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/10/27
"""

import os,sys,logging ,time,decimal,codecs
from ConfigParser import ConfigParser
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)

import ConnDb as Conn
import sqlite3 as lite


########################################################################
class SyncDb(object):
    """
    同步远端数据库的基类，
    同步sqlserver或是oracle数据库
    中的财务数据和市场数据都可继承该基类，
    然后针对数据库和数据表特点有针对的改写部分函数
    
    使用步骤如下
    
    step01:__init__
           #初始化
    step02:SetStartDate
           #设定同步数据的开始日期
    step03:ConvertDatesToMonths
           #将日期序列划分为月
    step04:ConvertDatesToQuarters
           #将日期序列划分为季度
    step05:LoadDataTableConfigs
           #读取需同步的数据表格的配置文件
    step06:ConnRmtDb
           #连接远端数据库
    step07:CheckRmtDb
           #检测远端数据库
             _CheckRmtTable
             #检测远端数据库中需同步的数据表
    step08:ConnLocalDb
           #连接本地数据库
    step09:CheckLocalDb
           #检测本地数据库
             _CheckLocalTable
             #检测本地数据库中需同步的数据表
    step10:Sync
           #开始同步数据
             _UpdateLocalTable
             #更新需同步的数据表                      
    """
    
    #----------------------------------------------------------------------
    def __init__(self,dbCfgFile,subClassName,logOutputHandler):
        """Constructor"""
        self.logger = logging.getLogger(subClassName)
        self.logger.setLevel(logging.DEBUG)
        for lh in logOutputHandler:
            self.logger.addHandler(lh)
            
        self.dbCfgFile = dbCfgFile
        self.logger.info("%s load config file: '%s'"%(__name__,self.dbCfgFile))
        configPath = "\\Configs\\DatabaseConfigs\\"
        self.dbCfg = ConfigParser()
        self.dbCfg.optionxform = str 
        self.dbCfg.read(root + configPath + dbCfgFile)
        
        
        
    #----------------------------------------------------------------------
    def SetStartDate(self,startDate):
        """"""
        self.startDate = startDate  
        self.currentDate = time.strftime("%Y%m%d")
        self.logger.info("Set start date, get current date")
        self.logger.info("Start date:{},Current date:{}".format(startDate,self.currentDate))
        
    
    #----------------------------------------------------------------------
    def ConvertDatesToMonths(self):
        """"""
        self.logger.info("Divide dates by month")
        monthRange = {"01":("0101","0131"),
                      "02":("0201","0230"),
                      "03":("0301","0331"),
                      "04":("0401","0430"),
                      "05":("0501","0531"),
                      "06":("0601","0630"),
                      "07":("0701","0831"),
                      "08":("0801","0831"),
                      "09":("0901","0930"),
                      "10":("1001","1031"),
                      "11":("1101","1130"),
                      "12":("1201","1231")}
        begMonth = self.startDate[0:6]
        endMonth = self.currentDate[0:6]
        begYear = self.startDate[0:4]
        endYear = self.currentDate[0:4]
        self.monthRange = []
        year = int(begYear)
        while year<=int(endYear):
            for m in ["01","02","03","04","05","06","07","08","09","10","11","12"]:
                month = str(year)+m
                if month==begMonth:
                    _mR1 = self.startDate
                    _mR2 = str(year)+monthRange[m][1]
                    self.monthRange.append((_mR1,_mR2))
                elif month>begMonth and month<endMonth:
                    _mR1 = str(year)+monthRange[m][0]
                    _mR2 = str(year)+monthRange[m][1]
                    self.monthRange.append((_mR1,_mR2))
                elif month==endMonth:
                    _mR1 = str(year)+monthRange[m][0]
                    _mR2 = self.currentDate
                    self.monthRange.append((_mR1,_mR2))
                else:
                    pass
            year+=1
                  
                    
    #----------------------------------------------------------------------
    def ConvertDatesToQuarters(self,endQuarter):
        """"""
        self.logger.info("Divide dates by quater")
        dateEOQ = {"1Q":("0101","0331"),
                   "2Q":("0401","0630"),
                   "3Q":("0701","0930"),
                   "4Q":("1001","1231")}
        rptName = {"1Q":"-Quarter1",
                   "2Q":"-Quarter2",
                   "3Q":"-Quarter3",
                   "4Q":"-Quarter4"}
        startYear = self.startDate[0:4]
        thisYear = self.currentDate[0:4]
        self.rptNameSeq = []
        self.quarterSeq = []
        year = int(startYear)
        while  year < int(thisYear):
            for k in [1,2,3,4]:
                self.quarterSeq.append((str(year)+dateEOQ[str(k)+"Q"][0],str(year)+dateEOQ[str(k)+"Q"][1]))
                self.rptNameSeq.append((str(year)+rptName[str(k)+"Q"]))      
            year+=1
        k = 1
        q = "1Q"
        while q <= endQuarter:
            self.quarterSeq.append((str(year)+dateEOQ[q][0],str(year)+dateEOQ[q][1]))
            self.rptNameSeq.append((str(year)+rptName[str(k)+"Q"]))  
            k+=1 
            q = str(k)+'Q'       


    #----------------------------------------------------------------------
    def LoadDataTableConfigs(self,tbCfgFile):
        """
        Data tables in remote database 
        needed to be synchronized    
        """
        self.logger.info("%s load config file: %s"%(__name__,tbCfgFile))
        self.tbCfg = ConfigParser()
        self.tbCfg.optionxform = str
        self.tbCfg.readfp(codecs.open(tbCfgFile,'r',"utf-8-sig"))
        self.tables = self.tbCfg.items("TableToSync")
        self.tableItems = {}
        for tb in self.tables:
            self.tableItems[tb[0]] = self.tbCfg.items("Items:"+tb[0])
             
            
    #----------------------------------------------------------------------
    def ConnRmtDb(self,dbName,dbStyle):
        """
        Connect to remote database
        """
        self.logger.info("Connect remote database")
        connRmtDb = Conn.ConnRmtDb(self.dbCfgFile,self.logger)
        self.conn = connRmtDb.Conn(dbName,dbStyle)
        self.dbStyle = dbStyle
    
    
    #----------------------------------------------------------------------
    def CheckRmtDb(self):
        """
        Check remote database
        """
        self.logger.info("Check remote database...")
        for tb in self.tables:
            self._CheckRmtTable(tb)        
        
        
    #----------------------------------------------------------------------
    def _CheckRmtTable(self,tb):
        """
        Check data table in remote database
        """
        self.logger.info("Check remote table: '%s'('%s')"%(tb[0],tb[1]))
        cur = self.conn.cursor()
        if self.dbStyle == "mssql":
            sql = """
                  SELECT TOP 1 CTIME FROM %s
                  ORDER BY CTIME DESC
                  """%tb[0]
            try:
                cur.execute(sql)
            except Exception,e:
                self.logger.error(e)
            row = cur.fetchone()
            dt = row[0].strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info("Last update time: %s"%dt)
    
    
    #----------------------------------------------------------------------
    def ConnLocalDb(self,dbName):
        """
        Connect to local sqlite database,
        if not exist, create it
        """
        self.logger.info("Connecting to local database '%s'"%dbName)
        lite.register_adapter(decimal.Decimal, lambda x:str(x))
        locDbPath = self.dbCfg.get("Local", "RawEquityData")
        if not os.path.exists(locDbPath+dbName):
            self.logger.info("Database '%s' dose not exist, create one"%dbName)
        self.locConn = lite.connect(locDbPath+dbName)
        self.locConn.text_factory = str
        #print "Connected to local database: '%s'\n"%dbName
        
        
    #----------------------------------------------------------------------
    def CheckLocalDb(self):
        """
        Check local database
        """
        self.logger.info("Check local database...")
        self.lastRecDate = {}
        for tb in self.tables:
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s'"%tb[1]
            cur = self.locConn.cursor()
            cur.execute(sql)
            row = cur.fetchone()
            if row == None:
                self.logger.info("Local table '%s' dose not exist"%tb[1])
                self._CreateLocalTable(tb)
            else:
                self._CheckLocalTable(tb)       
    
    
    #----------------------------------------------------------------------
    def _CheckLocalTable(self,tb):
        """
        Check data table in local database,
        if table not exist, create it
        """
        self.logger.info("Check local table: '%s'"%tb[1])
    
    
    #----------------------------------------------------------------------
    def _CreateLocalTable(self,tb):
        """
        Create local data tables
        """
        self.logger.info("Create local table: %s"%tb[1])
        sql = str(self.tbCfg.get("SQL:"+tb[0],"Create"))%tb[1]
        cur = self.locConn.cursor()
        cur.execute(sql)
        
        
    #----------------------------------------------------------------------
    def Sync(self,replace=1):
        """
        Start synchronization
        """
        self.logger.info("Syncronizing...")
        self.replace = replace
        if replace==1:
            for tb in self.tables:
                self.locConn.execute("DROP TABLE IF EXISTS %s"%tb[1])
                self.locConn.execute("DROP INDEX IF EXISTS %sINDEX"%tb[1])
                self._CreateLocalTable(tb)
                self._UpdateLocalTable(tb)
        else:
            for tb in self.tables:
                self._UpdateLocalTable(tb)
            
    
    #----------------------------------------------------------------------
    def _UpdateLocalTable(self,tb):
        """"""
        fSql = self.tbCfg.get("SQL:"+tb[0],"Fetch")
        print fSql        
        
        

