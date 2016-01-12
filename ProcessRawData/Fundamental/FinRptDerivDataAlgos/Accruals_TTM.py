#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/29
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算过去12月Accruals
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
    
    sql = """
           SELECT NetProfits
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
    if content[0]==None or content==None:
        return None
    v11 = content[0]
        
    cur.execute(sql.format(stkCode,lstAnnRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstAnnRptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None     
    if content[0]==None or content==None:
        return None
    v12 = content[0]
    
    cur.execute(sql.format(stkCode,lstSameRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstSameRptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None     
    if content[0]==None or content==None:
        return None
    v13 = content[0]
    
    sql = """
           SELECT NETCFO
           FROM CashFlowStatement
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
    v21 = content[0]

    cur.execute(sql.format(stkCode,lstAnnRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstAnnRptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None     
    if content[0]==None or content==None:
        return None
    v22 = content[0]

    cur.execute(sql.format(stkCode,lstSameRptDate,lookupDate))
    MyPrint(sql.format(stkCode,lstSameRptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None     
    if content[0]==None or content==None:
        return None
    v23 = content[0]    
        
    return v11-v21+v12-v22-v13+v23


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    