#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/20
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算NetBigOrderInFlowTotal10d
    """
    day = 10
    min_p = int(day*0.6666666667)
    orderDiff = pd.rolling_mean(df['BigOrderDiff'],day,min_periods=min_p)
    vol = pd.rolling_mean(df['vol'],day)
    res = (orderDiff/vol).to_frame("BigOrderDiff{}d".format(day))
    return res

