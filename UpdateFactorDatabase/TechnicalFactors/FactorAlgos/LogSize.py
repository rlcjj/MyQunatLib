#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/1
"""


import numpy as np


#----------------------------------------------------------------------
def Calc(df):
    """
    计算LogSize
    """
    mcap = np.log(df["price"]*df["f_cap"])
    res = mcap.to_frame("logsize")
    return res