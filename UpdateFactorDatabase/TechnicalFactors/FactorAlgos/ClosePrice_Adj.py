#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/7
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算日复权收盘价
    """
    cp = df["price_adj"]
    res = cp.to_frame("ClosePrice_Adj")
    return res