#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/12
"""

#----------------------------------------------------------------------
def Calc(cur,rptInfo,p,s,date,stkCode):
    """"""
    """
    计算过去12个月Asset turnover
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    finYear = rptInfo[0]
    decDate = rptInfo[1]       

    sql = """
          SELECT Sales_TTM*2/(TotalAssets+IFNULL(TotalAssets_1Y_Ago,TotalAssets))
          FROM FinancialPITData
          WHERE StkCode='{}'
                AND DeclareDate='{}'
          """
    cur.execute(sql.format(stkCode,decDate))
    content = cur.fetchone()
    if content==None:
        return None
    if content[0]==None:
        return None    
    v = content[0]
    #print v,s,p
    return v