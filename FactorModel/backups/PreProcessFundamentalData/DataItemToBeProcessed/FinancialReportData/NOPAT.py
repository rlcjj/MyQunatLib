#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/2/2
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算当期Net operating profit after tax,减法
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
                  -ifnull(AssetsDeval,0),
                  TotalProfits,
                  IncomeTax
           FROM IncomeStatement 
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """    
    
    sql2 = """
           SELECT OpProfits,
                  TotalProfits,
                  IncomeTax
           FROM IncomeStatement 
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """      
    
    if rptInfo[1]==1:
        sql = sql1
    else:
        sql = sql2
        
    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()
    if content==None:
        return None       
    if content[0]==None or content[1]==None  or content[2]==None:
        return None
    v11 = content[0]
    v12 = content[1]
    v13 = content[2]
        
    t = v13/v12
          
    return v11*(1-t)


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    