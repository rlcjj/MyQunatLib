#!/usr/bin/env python
#coding:utf-8
"""
  Author:  WUsf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/16
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算过去3个月内每股盈利
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """
    
    rptDate = rptInfo[0]
    rptYear = rptDate[0:4]
    rptMonth = rptDate[4:]
    if rptMonth == "0630":
        lstRptDate = rptYear+"0331"
    elif rptMonth == "0930":
        lstRptDate = rptYear+"0630"
    elif rptMonth == "1231":
        lstRptDate = rptYear+"0930"
    else:
        lstRptDate = rptDate
    
    sql ="""
          SELECT NetProfitToParent 
          FROM IncomeStatement 
          WHERE StkCode='{}'
          AND RPT_DATE='{}'
          AND RDeclareDate<='{}'
          ORDER BY RdeclareDate DESC
          """    
    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None       
    if content==None or content[0]==None:
        return None
    v1 = content[0]    
    
    cur.execute(sql.format(stkCode,lstRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstRptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None       
    if content==None or content[0]==None:
        return None
    v2 = content[0]
          
    if rptMonth == "0331":
        #print rptDate,v1
        return v1
    else:
        #print rptDate,lstRptDate,v1,v2
        return v1-v2


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg
    