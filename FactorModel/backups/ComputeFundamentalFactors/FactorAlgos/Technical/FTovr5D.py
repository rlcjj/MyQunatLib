#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf- --<>
  Purpose: 
  Created: 2016/4/29
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(cur,acctPeriods,p,s,date,stkCode):
    """"""
    """
    计算5日平均交易量比5日平均流通市值
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    sql = """
          SELECT Date
          FROM MktCap
          WHERE StkCode='{}'
                AND Date<='{}'
          ORDER BY Date DESC LIMIT 1       
          """
    cur.execute(sql.format(stkCode,begDate))
    content = cur.fetchone()
    _begDate = content[0]
    
    sql = """
          select s.date,s.[TC],c.[FLoatCap] from 
          (select * from astockdata 
          where StkCode='{}') s
          left join
          (select * from marketcap
          where StkCode='{}') c 
          on s.stkcode=c.stkcode and s.date=c.date
          where s.date<='{}' and s.date>='{}' order by s.date
          """
    cur.execute(sql.format(stkCode,stkCode,endDate,_begDate))
    content = cur.fetchall()
    df = pd.DataFrame(content)
    df = df.fillna(method='ffill')
    print df
    #print v,s,p,date[1],stkCode
    #print  v/s/p/10000.0
    return df