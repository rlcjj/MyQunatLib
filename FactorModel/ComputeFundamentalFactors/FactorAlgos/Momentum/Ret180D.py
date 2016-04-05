#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/21
"""

import numpy as np

#----------------------------------------------------------------------
def Calc(cur,acctPeriods,p,s,date,stkCode):
    """"""
    """
    计算180天收益 
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]

    sql = """
          SELECT TC_Adj
          FROM MktData
          WHERE StkCode='{}'
                AND Date<='{}'
          ORDER BY Date DESC LIMIT 180
          """
    cur.execute(sql.format(stkCode,endDate))
    content = cur.fetchall()
    if content==[]:
        return None
    if content[-1][0]==None or content[0][0]==None:
        return None
    v1 = content[0][0]
    v2 = content[-1][0]
    #print v,s,p
    return np.log(v1/v2)