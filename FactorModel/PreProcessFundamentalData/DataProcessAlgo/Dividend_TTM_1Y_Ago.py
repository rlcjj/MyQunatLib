#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/13
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算去年滚动12个月分红
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """    
    
    thisYear = lookupDate[0:4]
    lstYear = str(int(thisYear)-1)
    lst2Year = str(int(thisYear)-2)
    thisYearSemiRpt = thisYear + "0630"
    lstAnnRptDate = lstYear + "1231"
    lstSemiRptDate = lstYear + "0630"
    lst2AnnRptDate = lst2Year + "1231"
    lst2SemiRptDate = lst2Year + "0630"    
    
    sql =  """
           SELECT RPT_DATE
           FROM Dividend
           WHERE StkCode='{}'
           AND RDeclareDate<='{}'
           ORDER BY RDeclareDate DESC
           LIMIT 1
           """    
    cur.execute(sql.format(stkCode,lookupDate))
    MyPrint(sql.format(stkCode,lookupDate))
    content = cur.fetchone()
    if content == None:
        return None
    else:
        lastRptDate = str(int(content[0][0:4])-1)+content[0][4:]
        
    thisYear = str(int(lookupDate[0:4])-1)
    lstYear = str(int(thisYear)-1)
    lst2Year = str(int(thisYear)-2)
    thisYearSemiRpt = thisYear + "0630"
    lstAnnRptDate = lstYear + "1231"
    lstSemiRptDate = lstYear + "0630"
    lst2AnnRptDate = lst2Year + "1231"
    lst2SemiRptDate = lst2Year + "0630"  
    
    sql =  """
           SELECT RPT_DATE,RDeclareDate,DivRatio_Num
           FROM Dividend
           WHERE StkCode='{}'
           AND RPT_DATE<='{}'
           ORDER BY RDeclareDate DESC
           LIMIT 2
           """    
    cur.execute(sql.format(stkCode,lastRptDate))
    MyPrint(sql.format(stkCode,lastRptDate))
    content = cur.fetchall()
    if len(content)==0:
        return None
    if len(content)==1:
        if content[0][0]>=lst2AnnRptDate:
            return content[0][2]
        else:
            return None    

    v1 = content[0][2]+content[1][2]
    v2 = content[0][2]
    if content[0][0]==thisYearSemiRpt:
        if content[1][0]==lstAnnRptDate:
            v = v1
        else:
            v = v2
    elif content[0][0]==lstAnnRptDate:
        if content[1][0]==lstSemiRptDate:
            v = v1
        else:
            v = v2
    elif content[0][0]==lstSemiRptDate:
        if content[1][0]==lst2AnnRptDate:
            v = v1
        else:
            v = v2
    elif content[0][0]==lst2AnnRptDate:
        if content[1][0]==lst2SemiRptDate:
            v = v1
        else:
            v = v2
    else:
        return None
        
    return v


#----------------------------------------------------------------------
def MyPrint(arg):
    """"""
    _print = 0
    if _print == 1:
        print arg    