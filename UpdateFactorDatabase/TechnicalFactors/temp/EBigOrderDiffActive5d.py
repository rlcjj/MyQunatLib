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
    计算NetBigOrderInFlowActive5d
    """
    _netBigOrderInFlowActive = df['EBigOrderCashDiffActive']/df['amt']
    netBigOrderInFlowActive = pd.rolling_mean(_netBigOrderInFlowActive,5,min_periods=3)
    res = netBigOrderInFlowActive.to_frame("EBigOrderDiffActive5d")
    return res

