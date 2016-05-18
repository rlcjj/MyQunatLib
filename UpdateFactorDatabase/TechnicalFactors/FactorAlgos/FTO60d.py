#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/11
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算60日换手率
    """
    mv = pd.rolling_mean(df['vol'],60)
    mc = pd.rolling_mean(df['f_cap'],60)
    res = (mv/mc).to_frame("FTO60d")
    return res

