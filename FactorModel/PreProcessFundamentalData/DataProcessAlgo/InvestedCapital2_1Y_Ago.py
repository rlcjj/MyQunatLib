#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/20
"""

#----------------------------------------------------------------------
def Calc(cur,lookupDate,rptInfo,stkCode):
    """
    计算企业全部投入资本
    cur:内存数据库cursor
    lookupDate:查询当日的日期，返回是至该日期最后更新的信息
    lagDays:从lookupDate向前推lagDays天数，之前更新的信息不使用
    stkCode：股票代码
    """  

    rptDate = str(int(rptInfo[0][0:4])-1)+rptInfo[0][4:]
    sql1 = """
           SELECT TotLiab
                  -ifnull(AcctPayab,0)
                  -ifnull(AdvanceFromCustomers,0) 
                  -ifnull(PayrollPayab,0) 
                  -ifnull(OtherPayab,0)
                  -ifnull(AccruedExpns,0)
                  -ifnull(DeferredRevenue,0)
                  -ifnull(OtherCurLiab,0)
                  -TotNonCurLiab
                  +ifnull(LngTmBorrow,0)
                  +ifnull(BondPayab,0)
                  +TotShrHldEqtyIncludeMinor
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """
    sql2 = """
           SELECT ifnull(BorrFromCentralBank,0)
                  +ifnull(InterBankLending,0)
                  +ifnull(OtherFinInstituteDeposit,0)
                  +ifnull(FinAssetSoldForRepurchase,0)
                  +ifnull(TransacFinDebt,0)
                  +ifnull(BondPayab,0)
                  +TotShrHldEqtyIncludeMinor
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """
    sql3 = """
           SELECT ifnull(FinAssetSoldForRepurchase,0)
                  +ifnull(PolicyHolderDeposit,0)
                  +ifnull(LngTmBorrow,0)
                  +TotShrHldEqtyIncludeMinor
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """
    sql4 = """
           SELECT ifnull(ShtTmBorrow,0) 
                  +ifnull(ShrtTmFinBillPayab,0)
                  +ifnull(InterBankLending,0)
                  +ifnull(FinAssetSoldForRepurchase,0) 
                  +ifnull(LngTmBorrow,0) 
                  +ifnull(BondPayab,0)
                  +TotShrHldEqtyIncludeMinor
           FROM BalanceSheet
           WHERE StkCode='{}'
           AND RPT_DATE='{}'
           AND RDeclareDate<='{}'
           ORDER BY RdeclareDate DESC
           """   

    if rptInfo[1]==1:
        sql = sql1
    elif rptInfo[1]==2:
        sql = sql2
    elif rptInfo[1]==3:
        sql = sql3
    elif rptInfo[1]==4:
        sql = sql4
    else:
        print "Error, no type company"
    cur.execute(sql.format(stkCode,rptDate,lookupDate))
    #MyPrint(sql.format(stkCode,rptDate,lookupDate))
    content = cur.fetchone()  
    if content==None:
        return None       
    if content==None or content[0]==None or content[0]==0:
        return None
    return content[0]