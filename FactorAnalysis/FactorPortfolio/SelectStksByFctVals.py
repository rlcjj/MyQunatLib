#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
root = os.path.abspath(__file__).split("PyQuantStrategy")[0]+"PyQuantStrategy"
sys.path.append(root)
import FundamentalAnalysis.FdmtFactorVal.GetFdmtFactorVal as GetVal

########################################################################
class SelectStksByFctVals:
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finRptDataAddr,mktDataAddr):
        """Constructor"""
        self.getFctVal = GetVal.GetFdmtFactorVal(finRptDataAddr,mktDataAddr)
        
            
    #----------------------------------------------------------------------
    def Select(self,date,stkList,algo,ratio):
        """"""
        numOfStks = len(stkList)
        numSelected = int(ratio*numOfStks)
        
        stkFctVal = {}
        sortedStk = []
        for stk in stkList:
            v = self.getFctVal.GetVal(date,180,stk,algo)
            if v!=None:
                stkFctVal[stk] = v
        for _stk in sorted(stkFctVal, key=stkFctVal.get, reverse=True):
            #print _stk, stkFctVal
            sortedStk.append(_stk)
        
        return sortedStk[0:numSelected],sortedStk[-numSelected:],stkFctVal
        
        
        
        
    