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
    计算InstitutionalOrderInFlowTotal20d
    """
    day = 20
    min_p = int(day*0.6666666667)
    orderDiff = pd.rolling_mean(df['InstOrderDiff'],day,min_periods=min_p)
    vol = pd.rolling_mean(df['vol'],day)
    res = (orderDiff/vol).to_frame("InstOrderDiff{}d".format(day))
    return res

