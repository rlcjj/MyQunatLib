#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/21
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算公司一年前账面价值,即归属母公司权益的账面价值
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """    
    
    rptDate = str(int(rptInfo[0][0:4])-2)+rptInfo[0][4:]
    
    sql =  """
           SELECT ifnull(TotShrHldEqtyExcludeMinor,TotAssets-TotLiab)
           FROM BalanceSheet
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
    if content[0]==None or content==None:
        return None
    v1 = content[0]
        
    return v1


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    