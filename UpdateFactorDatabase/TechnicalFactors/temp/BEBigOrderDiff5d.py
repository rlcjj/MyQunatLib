#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/16
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算NetBigOrderInFlowTotal5d
    """
    _netBigOrderInFlowTotal = df['BigOrderCashDiffTotal2']/df['amt']
    netBigOrderInFlowTotal = pd.rolling_mean(_netBigOrderInFlowTotal,5,min_periods=3)
    res = netBigOrderInFlowTotal.to_frame("BEBigOrderDiffTotal5d")
    return res

