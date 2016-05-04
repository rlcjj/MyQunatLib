#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/6
"""

from datetime import datetime,timedelta

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算今年年报预期Earning
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """    
    
    today = datetime.strptime(lookupDate,"%Y%m%d")
    thisYear = lookupDate[0:4]
    thisRptDateStr = thisYear+"1231"
    thisRptDate = datetime.strptime(thisRptDateStr,"%Y%m%d")
    nextYear = str(int(lookupDate[0:4])+1)
    nextRptDateStr = nextYear+"1231"
    #daysToYearEnd = (thisRptDate-today).days
    #ratio = daysToYearEnd/365.0
    
    sql =  """
           SELECT EPS
           FROM ForecastData
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """    

    cur.execute(sql.format(stkCode,thisRptDateStr,lookupDate))
    MyPrint(sql.format(stkCode,thisRptDateStr,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None       
    if content[0]==None or content==None:
        return None
    v = content[0]
    
    return v


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    