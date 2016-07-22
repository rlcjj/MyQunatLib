#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/14
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算250日sharpe ratio
    """
    ret = np.log(df["price_adj"])-np.log(df["price_adj"].shift(1))
    _mean = pd.rolling_mean(ret,250)
    _std = pd.rolling_std(ret,250)
    res = (_mean/_std).to_frame("SharpeRatio250d")
    return res