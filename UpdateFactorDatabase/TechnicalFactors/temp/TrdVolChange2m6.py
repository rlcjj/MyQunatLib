#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wusifan221@gmial.com>
  Purpose: 
  Created: 2016/6/16
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算trading volume change 40d/120d
    """
    cshVol1 = pd.rolling_mean(df['amt'],40)
    cshVol2 = pd.rolling_mean(df['amt'],120)
    res = (cshVol1/cshVol2).to_frame("TrdVolChange2m6")
    return res

