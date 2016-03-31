#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<>
  Purpose: 
  Created: 2015/11/13
"""

import logging


#----------------------------------------------------------------------
def LogOutputHandler(fileName):
    """"""
    fh = logging.FileHandler(fileName)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s|%(name)s|%(levelname)s] %(message)s',
                                  "%Y-%m-%d %H:%M:%S")  
    fh.setFormatter(formatter)  
    ch.setFormatter(formatter)     
    return (fh,ch)