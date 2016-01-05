#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/30
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算企业带息债务,减法
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """  
    
    rptDate = rptInfo[0]
    sql = """
           SELECT TotLiab
                  -ifnull(AcctPayab,0)
                  -ifnull(AdvanceFromCustomers,0) 
                  -ifnull(PayrollPayab,0) 
                  -ifnull(OtherPayab,0)
                  -ifnull(AccruedExpns,0)
                  -ifnull(DeferredRevenue,0)
                  -ifnull(OtherCurLiab,0)
                  -TotNonCurLiab
                  +ifnull(LngTmBorrow,0)
                  +ifnull(BondPayab,0)
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """

    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    #MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None       
    if content==None or content[0]==None or content[0]==0:
        return None
    return rptDate,content[0]
        