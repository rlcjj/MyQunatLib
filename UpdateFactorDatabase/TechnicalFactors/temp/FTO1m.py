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
    计算20日换手率
    """
    mv = pd.rolling_mean(df['amt'],20)
    mc = pd.rolling_mean(df['f_cap']*df['price'],20)
    res = (mv/mc).to_frame("FTO1m")
    return res