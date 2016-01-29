#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/1/19
"""

import matplotlib.pyplot as plt
from matplotlib.ticker import FixedFormatter, MultipleLocator, FuncFormatter, NullFormatter
from matplotlib.finance import candlestick
from matplotlib.dates import date2num
from matplotlib.ticker import MultipleLocator
import numpy
import matplotlib.cm as cm
import numpy

#################################################################
#--------------------------------------------------------------------
def DrawCumulativeReturnCurve(dates, returns, chartName, path):
    lenOfDays = len(dates)
    x = range(0,lenOfDays)
    #print len(x),lenOfDays
    cumReturns = []
    __cumRet = 0
    for ret in returns[0:]:
        __cumRet += ret
        cumReturns.append(__cumRet)
    maxCumReturns = []
    __maxCumRet = 0
    for ret in cumReturns:
        if ret >= __maxCumRet:
            __maxCumRet = ret
        maxCumReturns.append(__maxCumRet)

    dd = []
    for ret1,ret2 in zip(cumReturns, maxCumReturns):
        dd.append(ret2-ret1)
    mdd = max(dd)
    volatility = numpy.std(returns)
    totalReturns = numpy.sum(returns)
    annVol = volatility*numpy.sqrt(250)
    annRet = totalReturns/lenOfDays*250
    sharpeRatio = (annRet-0.04)/annVol
    #RollingSum20 = RollingApply(sum, returns, lookbackWindow1)
    #RollingSum30 = RollingApply(sum, returns, lookbackWindow2)

    fig  = plt.figure(num=None, figsize=(16,8), dpi=100, facecolor=None, 
                      edgecolor=None, frameon=True)
    ax = fig.add_subplot(111)
    ax.set_axis_bgcolor('lightgoldenrodyellow')
    #xlocator = MultipleLocator(1)
    ylocator = MultipleLocator(0.05)
    #y2locator = MultipleLocator(0.01)
    #ax.xaxis.set_minor_locator(xlocator)
    ax.yaxis.set_major_locator(ylocator)
    #ax.yaxis.set_minor_locator(y2locator)
    plt.xlim(0,lenOfDays+1)
    plt.ylim(min(cumReturns)-0.01, max(cumReturns)+0.01)
    interval = lenOfDays/30
    plt.xticks(x[::interval],dates[::interval],rotation=90, size=6,color='black')
    plt.yticks(size = 8, color='black')
    plt.xlabel('Date',color='black',size = 10)
    plt.ylabel('Cumulative Returns', color='black',size = 10)
    plt.title(chartName, color = 'black',fontweight="bold")
    plt.text(int(x[-1]*0.8),max(cumReturns)+0.3 , '$r=$%6.2f%%\n$\sigma=$%6.2f%%\n$sr=$%6.2f'
             %(annRet*100,annVol*100,sharpeRatio),fontsize = 10, color='black',fontweight="bold")
    plt.grid(b=True, which='both')

    plt.plot(cumReturns, label='Cumulative Return', color = 'blue',linewidth=0.5)
    #plt.plot(RollingSum20, label='RollingSum60', color = 'cyan')
    #plt.plot(RollingSum30, label='RollingSum250', color = 'pink')
    #plt.plot(numpy.zeros(lenOfDays),color='black',linestyle='.')
    plt.legend(loc='upper left',fontsize = 8, frameon=True)

    k = 0
    kk = 0
    for i in range(2,lenOfDays):
        if dd[i]>max(dd[0:i]):
            k = i

    for ii in range(2,k):
        if cumReturns[ii]>max(cumReturns[0:ii]):
            kk = ii
    b=k
    bb=kk
    plt.plot([bb,bb],[cumReturns[b],cumReturns[bb]],color='r',linestyle='-.',linewidth=1)
    plt.plot([bb,b],[cumReturns[b],cumReturns[b]],color='r',linestyle='-.',linewidth=1)
    plt.scatter(bb, cumReturns[bb], s=30, c='b', marker='x',color='r')
    plt.scatter(b, cumReturns[b], s=30, c='b', marker='x',color='r')
    plt.text(bb,cumReturns[b],'$mdd=$%6.2f%%'%(mdd*100),fontsize=10,color='black')
    #plt.show()
    plt.savefig(path,dpi=100,format="jpeg")
    plt.close(fig)    
    
    
    
#################################################################
#----------------------------------------------------------------
def mean(arr):
    return sum(arr)/len(arr)

#################################################################
#----------------------------------------------------------------
def RollingApply(func, arr, window):
    result = []
    for i in xrange(len(arr)):
        if i < window - 1:
            result.append(numpy.nan)
        else:
            result.append(func(arr[i+1-window:i+1]))
    return result