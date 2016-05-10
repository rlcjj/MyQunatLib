#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/10
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算5日换手率
    """
    mv = pd.rolling(df['vol'],5)
    mc = pd.rolling(df['f_cap'],5)
    res = (mv/mc).to_frame("FTO5d")
    return res