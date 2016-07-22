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
    计算trading volume change 60d/120d
    """
    cshVol1 = pd.rolling_mean(df['amt'],60)
    cshVol2 = pd.rolling_mean(df['amt'],120)
    res = (cshVol1/cshVol2).to_frame("TrdVolChange3m6")
    return res

