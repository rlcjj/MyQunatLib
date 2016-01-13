#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/13
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算财务报表期间Earning
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays，之前更新的信息不使用
    stkCode：股票代码
    """
    
    rptDate = rptInfo[0]

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
    if content==None:
        return None       
    if content==None or content[0]==None:
        return None
    v1 = content[0]

    return v1


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg
    