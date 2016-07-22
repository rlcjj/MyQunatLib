#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/17
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算资金净流入率5日平均
    """
    _netInFlow = df['NetInFlowRatio']
    netInFlow = pd.rolling_mean(_netInFlow,5,min_periods=3)
    res = netInFlow.to_frame("NetInFlowRatio5d")
    return res

