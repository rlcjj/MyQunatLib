#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/1
"""

import numpy as np
import pandas as pd


#----------------------------------------------------------------------
def Calc(df):
    """
    计算未来20天收益
    """
    fr = (df["price_adj"].shift(-20)-df["price_adj"])/df["price_adj"]
    res = fr.to_frame("FRet20")
    return res