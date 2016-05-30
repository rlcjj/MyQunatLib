#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/4/26
"""


import numpy as np


#----------------------------------------------------------------------
def Calc(cur,acctPeriods,p,s,date,stkCode):
    """"""
    """
    计算log(float cap * p)
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    sql = """
          SELECT FloatCap
          FROM MktCap
          WHERE StkCode='{}'
                AND Date<='{}'
          ORDER BY Date DESC LIMIT 1
          """
    cur.execute(sql.format(stkCode,endDate))
    content = cur.fetchone()
    if content==None:
        return None
    if content[0]==None:
        return None    
    v = content[0]
    #print v,s,p,date[1],stkCode
    #print  v/s/p/10000.0
    return np.log(v*p)