#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/26
"""

import matplotlib.pyplot as pl
import numpy as np


#----------------------------------------------------------------------
def ScatterPlot(x,y,v,xlabel,ylabel,title,path):
    """"""
    pl.cla()
    pl.clf()    
    
    _v = np.array(v)
    p1 = round(np.nanpercentile(_v,75),2)
    p2 = round(np.nanpercentile(_v,50),2)
    p3 = round(np.nanpercentile(_v,20),2)
    print p1,p2,p3
    
    _s = []
    _c = []
    for i in xrange(len(x)):
        _s.append(3)
        if _v[i]>=p1:
            _c.append('red')
        elif _v[i]<p1 and v[i]>=p2:
            _c.append('orange')
        elif v[i]<p2 and v[i]>=p3:
            _c.append('sage')
        elif _v[i]<p3:
            _c.append('green')
        else:
            _c.append('white')    

    pl.scatter(x,y,c=_c,s=_s,edgecolors='None')
    pl.xlabel(xlabel,color='black',size = 15)
    pl.ylabel(ylabel, color='black',size = 15)     
    pl.title(title)
    pl.grid()

    #for i in xrange((len(x))):
    #    print x[i],y[i],v[i],_c[i]
                   
    pl.savefig(path,dpi=100,format="jpeg")
    pl.close()    
    return _c