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
    计算20日Bollinger value
    """
    priceMean = pd.rolling_mean(df["price_adj"],20)
    priceStd = pd.rolling_std(df["price_adj"],20)
    bValue = (df["price_adj"]-priceMean)/priceStd
    res = bValue.to_frame("Bollinger20d")
    return res