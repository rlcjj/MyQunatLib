#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/6
"""


import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算当日收益
    """
    ret = np.log(df["price_adj"])-np.log(df["price_adj"].shift(1))
    res = ret.to_frame("Ret")
    return res