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
    计算250日换手率
    """
    mv = pd.rolling_mean(df['amt'],250)
    mc = pd.rolling_mean(df['f_cap']*df['price'],250)
    res = (mv/mc).to_frame("FTO12m")
    return res
