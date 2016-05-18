#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/16
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算60日平均总市值
    """
    mcap = df["price"]*df["t_cap"]
    res = pd.rolling_mean(mcap,60).to_frame("tcap60")
    return res