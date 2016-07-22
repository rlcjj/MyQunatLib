#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/15
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算300日相对000300指数的Beta
    """
    ret1 = np.log(df["price_adj"])-np.log(df["price_adj"].shift(1))
    ret2 = np.log(df["I000300"])-np.log(df["I000300"].shift(1))
    corr = pd.rolling_corr(ret1,ret2,300)
    std1 = pd.rolling_std(ret1,300)
    std2 = pd.rolling_std(ret2,300)
    res = (corr*std1/std2).to_frame("Beta000300_300d")
    return res