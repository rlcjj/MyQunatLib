#!/usr/bin/env python
#coding:utf-8
"""
  Author:  WUsf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/15
"""

import os,sys,logging 
from ConfigParser import ConfigParser
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)

#----------------------------------------------------------------------
def GetLocalDatabasePath():
    """"""
    configPath = "\\Configs\\DatabaseConfigs\\DbInfo.cfg"
    dbCfg = ConfigParser()
    dbCfg.optionxform = str 
    dbCfg.read(root + configPath)    
    return {"RawEquity":dbCfg.get("Local", "RawEquityData"),
            "ProcEquity":dbCfg.get("Local", "ProcessedEquityData")}
