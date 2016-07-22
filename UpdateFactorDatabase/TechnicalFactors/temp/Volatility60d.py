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
    计算60日波动率
    """
    ret = np.log(df["price_adj"])-np.log(df["price_adj"].shift(1))
    res = pd.rolling_std(ret,60).to_frame("Volatility60d")
    return res