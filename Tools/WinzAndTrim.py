#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/4/13
"""

import numpy as np


#----------------------------------------------------------------------
def Wins(mat,percent):
    """
    Winsorize matrix
    """
    upBund = np.nanpercentile(mat,percent,axis=0,interpolation='lower')
    dnBund = np.nanpercentile(mat,100-percent,axis=0,interpolation='higher')
    matMask = np.ma.masked_where(mat>upBund,mat)
    newMat = np.ma.filled(matMask,upBund)
    matMask = np.ma.masked_where(newMat<dnBund,newMat)
    newMat = np.ma.filled(matMask,dnBund)
    return newMat
    
    
#----------------------------------------------------------------------
def Trim(mat,percent):
    """
    Trim matrix
    """
    upBund = np.nanpercentile(mat,percent,axis=0,interpolation='lower')
    dnBund = np.nanpercentile(mat,100-percent,axis=0,interpolation='higher')
    matMask = np.ma.masked_where(mat>upBund,mat)
    newMat = np.ma.filled(matMask,np.nan)
    matMask = np.ma.masked_where(newMat<dnBund,newMat)
    newMat = np.ma.filled(matMask,np.nan)
    return newMat


#----------------------------------------------------------------------
def Standardize(mat,std):
    """"""
    _std = np.nanstd(mat,0)
    #_std[_std==0]=99999999
    _mean = np.nanmean(mat,0)
    _mat = (mat-_mean)/_std
    _mat[_mat>std] = std
    _mat[_mat<-std] = -std
    return _mat
    
    
    
    
if __name__ == "__main__":
    a = [[1,2],[2,3],[4,5],[4,5],[4,5],[4,5],[np.nan,5],[4,5],[4,5],[4,5],[5,6],[np.NAN,np.nan],[112,1],[11,2],[-312,-332],[223,122]]
    aa = np.array(a)
    bb = Trim(aa, 90)
    cc = Standardize(bb,3)
    f = open('test.csv','w')
    for i in range(len(aa)):
        a=aa[i]
        b=bb[i]
        c=cc[i]
        f.write("{},{},{},{},{},{}\n".format(a[0],a[1],b[0],b[1],c[0],c[1]))
    f.close()
    
    vv = np.hstack((bb,cc))
    print vv
    
    
    #cc = sp.mstats.winsorize(bb,limits=[0.1,0.1],inclusive=[False,False],axis=0)
    #for i in xrange(len(a)):
    #    print aa[i],bb[i],cc[i]

        
    