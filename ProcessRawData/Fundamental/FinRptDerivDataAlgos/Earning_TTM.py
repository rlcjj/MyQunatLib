#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 计算过去12个月区间Earning
  Created: 2015/12/2
"""

from datetime import datetime,timedelta

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算过去12个月区间Earning
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays，之前更新的信息不使用
    stkCode：股票代码
    """
    
    rptDate = rptInfo[0]
    rptYear = rptDate[0:4]
    rptMonth = rptDate[4:]
    lstYear = str(int(rptYear)-1)
    lstAnnRptDate = lstYear + "1231"
    lstSameRptDate = lstYear + rptMonth

    sql ="""
          SELECT NetProfitToParent 
          FROM IncomeStatement 
          WHERE StkCode='{}'
          AND RPT_DATE='{}'
          AND RDeclareDate<='{}'
          ORDER BY RDeclareDate DESC
          """
    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()
    if content==None or content[0]==None:
        return None
    v1 = content[0]

    cur.execute(sql.format(stkCode,lstAnnRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstAnnRptDate,lookupDate))
    content = cur.fetchone()
    if content==None or content[0]==None:
        return None
    v2 = content[0]
 
    cur.execute(sql.format(stkCode,lstSameRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstSameRptDate,lookupDate))
    content = cur.fetchone()
    if content==None or content[0]==None:
        return None
    v3 = content[0]    
    return rptDate,v1+v2-v3


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg
    