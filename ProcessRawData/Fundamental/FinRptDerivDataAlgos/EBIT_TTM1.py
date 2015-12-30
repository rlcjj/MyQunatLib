#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/10
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算过去12月内EBIT,减法
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """    
    
    rptDate = rptInfo[0]
    rptYear = rptDate[0:4]
    rptMonth = rptDate[4:]
    lstYear = str(int(rptYear)-1)
    lstAnnRptDate = lstYear + "1231"
    lstSameRptDate = lstYear + rptMonth
    
    sql1 = """
           SELECT OpRevenue
                  -TaxAndSurcharge
                  -OpCost
                  -SellExpns
                  -AdminExpns
                  -ifnull(AssetsDeval,0)
           FROM IncomeStatement 
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """    
    
    sql23 = """
           SELECT OpProfit
           FROM IncomeStatement 
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """      
    
    sql4 = """
           SELECT OpRevenue
                  -TaxAndSurcharge
                  -AdminExpns
                  -OtherBusinessCost
           FROM IncomeStatement 
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """    
    
    if rptInfo[1]==1:
        sql = sql1
    elif rptInfo[1]==4:
        sql = sql4
    else:
        sql = sql23
        
    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()
    if content[0]==None or content==None:
        return None
    v1 = content[0]
        
    cur.execute(sql.format(stkCode,lstAnnRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstAnnRptDate,lookupDate))
    content = cur.fetchone()
    if content[0]==None or content==None:
        return None
    v2 = content[0]
    
    cur.execute(sql.format(stkCode,lstSameRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstSameRptDate,lookupDate))
    content = cur.fetchone()
    if content[0]==None or content==None:
        return None
    v3 = content[0]
        
    return rptDate,(v1+v2-v3)


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    