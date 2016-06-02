#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/12
"""

#----------------------------------------------------------------------
def Calc(cur,rptInfo,p,s,date,stkCode):
    """
    计算CapitalEmployed/EV
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    finYear = rptInfo[0]
    decDate = rptInfo[1]      
    
    sql = """
          SELECT CapitalEmployed,
                 TotalDebt
                -Cash
                +IFNULL(PreferStock,0)
                +IFNULL(Eqty2Minor,0)
          FROM FinancialPITData
          WHERE StkCode='{}'
                AND DeclareDate='{}'
          """
    cur.execute(sql.format(stkCode,rptInfo))
    content = cur.fetchone()
    if content==None:
        return None
    if content[0]==None or content[1]==None:
        return None    
    v1 = content[0]
    v2 = content[1]
    
    #print v1,v2,s,p
    return v1/((s*p*10000.0)+v2)