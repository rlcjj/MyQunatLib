#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,datetime
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


########################################################################
class TradeDays(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self,dbName="MktGenInfo\\TradeDate_Wind_CICC.db"):
        """Constructor"""
        self.dbName = dbName
        
        
    #----------------------------------------------------------------------
    def GetTradeDays(self):
        """"""
        locDbPath = GetPath.GetLocalDatabasePath()
        trdDateDbAddr = locDbPath["EquityDataRaw"]+self.dbName
        conn = lite.connect(trdDateDbAddr)
        conn.text_factory = str
        cur = conn.cursor()
        cur.execute("SELECT TrdDate FROM TradeDate ORDER BY TrdDate ASC")
        rows = cur.fetchall()
        trdDay = []
        for row in rows:
            trdDay.append(row[0])
        self.trdDay = trdDay
        return trdDay        
        
        
    #----------------------------------------------------------------------
    def GetRebalanceDays(self,begDate,endDate,freq,rebalanceDay):
        """"""
        rebalanceDays = []
        for d in self.trdDay:
            if d>=begDate:
                startDay = d
                break
            
        if freq == "weekly":
            k = 1
            thisWeek = datetime.date(int(startDay[0:4]),int(startDay[4:6]),int(startDay[6:8])).isocalendar()[1]
            for d in self.trdDay:
                if d>startDay and d<=endDate:
                    if datetime.date(int(d[0:4]),int(d[4:6]),int(d[6:8])).isocalendar()[1]==thisWeek:
                        k+=1
                    else:
                        k=1
                        thisWeek=datetime.date(int(d[0:4]),int(d[4:6]),int(d[6:8])).isocalendar()[1]
                    if k==rebalanceDay:
                        rebalanceDays.append(d)        
            
        if freq == "monthly":
            k = 1
            thisMonth = startDay[4:6]
            for d in self.trdDay:
                if d>startDay and d<=endDate:
                    if d[4:6]==thisMonth:
                        k+=1
                    else:
                        k=1
                        thisMonth=d[4:6]
                    if k==rebalanceDay:
                        rebalanceDays.append(d)
                        
        if freq == "quarterly":
            k = 1
            thisQuarter = (int(startDay[4:6])-1)/3
            for d in self.trdDay:
                if d>startDay and d<=endDate:
                    if (int(d[4:6])-1)/3==thisQuarter:
                        k+=1
                    else:
                        k=1
                        thisQuarter=(int(d[4:6])-1)/3
                    if k==rebalanceDay:
                        rebalanceDays.append(d)
    
        if freq == "halfyearly":
            k = 0
            for d in self.trdDay:
                if d>startDay and d<=endDate:
                    if int(d[4:6]) == 4 or int(d[4:6]) == 10:
                        k+=1
                    else:
                        k=0
                if k==rebalanceDay:
                    rebalanceDays.append(d)
                    
                    
        if freq == "yearly":
            k = 0
            thisYear = startDay[4:6]
            for d in self.trdDay:
                if d>startDay and d<=endDate:
                    if d[0:4]== thisYear:
                        k+=1
                    else:
                        k=0
                        thisYear = d[0:4]
                if k==rebalanceDay:
                    rebalanceDays.append(d)                    
        return rebalanceDays    
    
    
    

                        
        
                        
                    
                    
                    
                    
            
        
    
    



if __name__ == "__main__":
    trdDay = GetTradeDays()
    print trdDay