#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys
import sqlite3 as lite
import Configs.RootPath as Root
import Tools.GetLocalDatabasePath as GetPath
RootPath=Root.RootPath

#----------------------------------------------------------------------
def GetTradeDays(dbName="MktGenInfo\\TradeDate_Wind_CICC.db"):
    """"""
    locDbPath = GetPath.GetLocalDatabasePath()
    trdDateDbAddr = locDbPath["EquityDataRaw"]+dbName
    conn = lite.connect(trdDateDbAddr)
    conn.text_factory = str
    cur = conn.cursor()
    cur.execute("SELECT TrdDate FROM TradeDate ORDER BY TrdDate ASC")
    rows = cur.fetchall()
    trdDay = []
    for row in rows:
        trdDay.append(row[0])
    return trdDay


if __name__ == "__main__":
    trdDay = GetTradeDays()
    print trdDay