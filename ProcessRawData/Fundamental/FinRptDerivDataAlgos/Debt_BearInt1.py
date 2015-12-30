#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/11
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算企业带息债务,加法
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """  
    
    rptDate = rptInfo[0]
    sql1 = """
           SELECT ifnull(ShtTmBorrow,0) 
                  +ifnull(NotesPayab,0)
                  +ifnull(NonCurLiabWithin1Year,0) 
                  +ifnull(LngTmBorrow,0) 
                  +ifnull(BondPayab,0)
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """
    sql3 = """
           SELECT ifnull(ShtTmBorrow,0) 
                  +ifnull(BorrFromCentralBank,0)
                  +ifnull(OtherFinInstituteDeposit,0)
                  +ifnull(BorrFromOtherFinInst,0) 
                  +ifnull(FinAssetSoldForRepurchase,0) 
                  +ifnull(LngTmBorrow,0) 
                  +ifnull(PolicyHolderDeposit,0)
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """            
    sql4 = """
           SELECT ifnull(ShtTmBorrow,0) 
                  +ifnull(ShrtTrmFinBillPayb,0)
                  +ifnull(BorrFromOtherFinInst,0)
                  +ifnull(FinAssetSoldForRepurchase,0) 
                  +ifnull(LngTmBorrow,0) 
                  +ifnull(BondPayab,0)
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """    
    if rptInfo[1]==1:
        sql = sql1
    elif rptInfo[1]==2:
        return None
    elif rptInfo[1]==3:
        sql = sql3
    elif rptInfo[1]==4:
        sql = sql4
    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    #MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()    
    if content==None or content[0]==None or content[0]==0:
        return None
    return rptDate,content[0]
        