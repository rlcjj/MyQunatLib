#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2015/12/17
"""

import os,sys,logging ,time,decimal,codecs
import sqlite3 as lite
root = os.path.abspath(__file__).split("MyQuantLib")[0]+"MyQuantLib"
sys.path.append(root)
import FactorAnalysis.TestRawFactorPortfolio.GetFactorVal as GetVal

########################################################################
class SelectStksByFctVals:
    """"""

    #----------------------------------------------------------------------
    def __init__(self,finRptDataAddr,mktDataAddr):
        """Constructor"""
        self.getFctVal = GetVal.GetFactorVal(finRptDataAddr,mktDataAddr)
        
            
    #----------------------------------------------------------------------
    def Select(self,date,stkList,algo,ratio,seq):
        """"""
        numOfStks = len(stkList)
        numSelected = int(ratio*numOfStks)
        
        stkFctVal = {}
        sortedStk = []
        port = {}
        for stk in stkList:
            v = self.getFctVal.GetVal(date,180,stk,algo)
            if v!=None:
                if v[0]!=None:
                    stkFctVal[stk] = v[0]
        for _stk in sorted(stkFctVal, key=stkFctVal.get, reverse=True):
            #print _stk, stkFctVal
            sortedStk.append(_stk)
        #print sortedStk[0],stkFctVal[sortedStk[0]],sortedStk[-1],stkFctVal[sortedStk[-1]]
        if seq == 1:
            port["long"] = sortedStk[0:numSelected]
            port["short"] = sortedStk[-numSelected:]
        else:
            port["long"] = sortedStk[-numSelected:]
            port["short"] = sortedStk[0:numSelected]
        return port,stkFctVal
        
        
        
        
    