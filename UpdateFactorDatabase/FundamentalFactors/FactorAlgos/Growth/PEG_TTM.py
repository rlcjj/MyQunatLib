#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/13
"""

#----------------------------------------------------------------------
def Calc(cur,acctPeriods,p,s,date,stkCode):
    """"""
    """
    计算过去12月内PEG
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    sql = """
          SELECT NetProfits2Parent_TTM
          FROM FinancialPITData
          WHERE StkCode='{}'
                AND DeclareDate>='{}'
                AND DeclareDate<='{}'
          ORDER BY DeclareDate DESC LIMIT 1
          """
    cur.execute(sql.format(stkCode,begDate,endDate))
    content = cur.fetchone()
    if content==None:
        return None
    if content[0]==None:
        return None    
    v1 = content[0]
    
    sql = """
          SELECT ForecastNextYearEPS/ForecastThisYearEPS-1
          FROM ForecastPITData
          WHERE StkCode='{}'
                AND DeclareDate>='{}'
                AND DeclareDate<='{}'
          ORDER BY DeclareDate DESC LIMIT 1
          """
    cur.execute(sql.format(stkCode,begDate,endDate))
    content = cur.fetchone()
    if content==None:
        return None
    if content[0]==None:
        return None    
    v2 = content[0]+0.0001   
    
    #print v1,v2,s,p/(v1/s/10000.0),v1/s/10000.0
    return p/(v1/s/10000.0)/v2/100