#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

from datetime import datetime,timedelta


#----------------------------------------------------------------------
def Calc(cur,date,stkCode):
    """"""
    """
    计算过去12月内E/P
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    sql = """
          SELECT EPS_TTM 
          FROM FinRptDerivData
          WHERE StkCode='{}'
                AND DeclareDate>='{}'
                AND DeclareDate<='{}'
          ORDER BY DeclareDate DESC LIMIT 1
          """
    cur.execute(sql.format(stkCode,begDate,endDate))
    content = cur.fetchone()
    if content==None or content[0]==None:
        return None    
    v1 = content[0]
    
    sql ="""
         SELECT TC 
         FROM MktData
         WHERE StkCode='{}'
         AND Date<='{}'
         ORDER BY Date DESC LIMIT 1
         """        
    cur.execute(sql.format(stkCode,endDate))
    content = cur.fetchone()
    if content==None or content[0]==None:
        return None    
    v2 = content[0]    
    #print stkCode,v1,v2
    return v1/v2