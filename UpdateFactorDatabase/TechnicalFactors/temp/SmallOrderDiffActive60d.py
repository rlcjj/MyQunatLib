#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/17
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算SmallOrderInFlowActive60d
    """
    _netSmallOrderInFlowActive = df['SmallOrderCashDiffActive']/df['amt']
    netSmallOrderInFlowActive = pd.rolling_mean(_netSmallOrderInFlowActive,60,min_periods=40)
    res = netSmallOrderInFlowActive.to_frame("SmallOrderDiffActive60d")
    return res

