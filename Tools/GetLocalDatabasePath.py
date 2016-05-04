#!/usr/bin/env python
#coding:utf-8
"""
  Author:  WUsf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/15
"""

import Configs.RootPath as Root
from ConfigParser import ConfigParser

RootPath = Root.RootPath

#----------------------------------------------------------------------
def GetLocalDatabasePath():
    """"""
    configPath = "\\Configs\\DatabaseInfo.cfg"
    dbCfg = ConfigParser()
    dbCfg.optionxform = str 
    dbCfg.read(RootPath + configPath)    
    return {"EquityDataRaw":dbCfg.get("Local", "EquityDataRaw"),
            "EquityDataRefined":dbCfg.get("Local", "EquityDataRefined")}
