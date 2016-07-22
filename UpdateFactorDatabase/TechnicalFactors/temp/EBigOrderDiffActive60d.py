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
    计算NetBigOrderInFlowActive60d
    """
    _netBigOrderInFlowActive = df['EBigOrderCashDiffActive']/df['amt']
    netBigOrderInFlowActive = pd.rolling_mean(_netBigOrderInFlowActive,60,min_periods=40)
    res = netBigOrderInFlowActive.to_frame("EBigOrderDiffActive60d")
    return res

