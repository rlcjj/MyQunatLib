#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/10/27
"""

import os,sys
import pyodbc,cx_Oracle
from ConfigParser import ConfigParser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"\\PyQuantStrategy"
sys.path.append(root)

########################################################################
class ConnRmtDb(object):
    """
    Connect to remote database
    """

    #----------------------------------------------------------------------
    def __init__(self, dbCfgFile,logger):
        """Constructor"""
        
        self.logger = logger
        self.logger.info("%s load config file: '%s'"%(__name__,dbCfgFile))
        configPath = "\\Configs\\DatabaseConfigs\\"
        self.cfg = ConfigParser()
        self.cfg.read(root + configPath + dbCfgFile)
        #print self.cfg.sections()
        
    #----------------------------------------------------------------------
    def Conn(self, rmtDbName, dbStyle):
        """"""
        self.server = self.cfg.get(rmtDbName, "server")
        self.usr = self.cfg.get(rmtDbName, "username")
        self.pwd = self.cfg.get(rmtDbName, "password")
        self.logger.info("Connecting to remote database: '%s'..."%rmtDbName)
        self.logger.info("  Server: '%s'"%self.server)
        self.logger.info("  Username: '%s'"%self.usr)
        self.logger.info("  Password: '%s'"%self.pwd)
        
        if dbStyle == "oracle":
            try:
                conn = cx_Oracle.connect(self.usr,self.pwd,self.server)
            except Exception,e:
                print e[1]
                raise EOFError
            self.logger.info("Successfully connected!")
        
        if dbStyle == "mssql":
            try:
                conn = pyodbc.connect("""
                                      DRIVER={SQL SERVER};
                                      SERVER=%s;DATABASE=Pgenius;
                                      UID=%s;PWD=%s;"""
                                      %(self.server,self.usr,self.pwd))
            except Exception,e:
                self.logger.error(e[1])
                raise EOFError
            self.logger.info("Successfully connected!")
        return conn       
        
        
    
