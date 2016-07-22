#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/6/15
"""


#----------------------------------------------------------------------
def Calc(df):
    """
    指数价格
    """
    res = df["I000300"].to_frame("IndexPrice000300")
    return res