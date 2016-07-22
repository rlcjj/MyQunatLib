#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/17
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算60日平均流动市值
    """
    mcap = df["price"]*df["f_cap"]
    res = mcap.to_frame("FCap1")
    return res