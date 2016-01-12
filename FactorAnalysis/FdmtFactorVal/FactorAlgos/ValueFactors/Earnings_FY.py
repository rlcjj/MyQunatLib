#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<>
  Purpose: 
  Created: 2016/1/12
"""

from datetime import datetime

#----------------------------------------------------------------------
def Calc(cur,acctPeriods,p,s,date,stkCode):
    """"""
    """
    计算未来12个月的预测每股盈利
    cur:内存数据库cursor
    date:查询当日的日期和数据有效的最早日期
    stkCode：股票代码
    """      
    begDate = date[0]
    endDate = date[1]
    
    today = datetime.strptime(endDate,"%Y%m%d")
    thisYear = endDate[0:4]
    thisRptDateStr = thisYear+"1231"
    thisRptDate = datetime.strptime(thisRptDateStr,"%Y%m%d")
    nextYear = str(int(thisYear)+1)
    daysToYearEnd = (thisRptDate-today).days
    ratio1 = daysToYearEnd/365.0   
    ratio2 = 1 - ratio1
    
    
    sql = """
          SELECT ForecastThisYearEPS*{}
                +ForecastNextYearEPS*{}
          FROM FcstData
          WHERE StkCode='{}'
                AND DeclareDate>='{}'
                AND DeclareDate<='{}'
          ORDER BY DeclareDate DESC LIMIT 1
          """
    cur.execute(sql.format(ratio1,ratio2,stkCode,begDate,endDate))
    content = cur.fetchone()
    if content==None:
        return None
    if content[0]==None:
        return None    
    v = content[0]
    print v,s,p
    return v/s/p/10000.0