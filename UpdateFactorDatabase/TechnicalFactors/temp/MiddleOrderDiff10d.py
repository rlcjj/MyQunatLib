#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wusifan221@gmail.com>
  Purpose: 
  Created: 2016/6/20
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算MiddleOrderInFlowTotal10d
    """
    day = 10
    min_p = int(day*0.6666666667)
    orderDiff = pd.rolling_mean(df['MiddleOrderDiff'],day,min_periods=min_p)
    vol = pd.rolling_mean(df['vol'],day)
    res = (orderDiff/vol).to_frame("MiddleOrderDiff{}d".format(day))
    return res

