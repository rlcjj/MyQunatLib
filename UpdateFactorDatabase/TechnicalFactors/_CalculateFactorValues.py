#!/usr/bin/env python
#coding:utf-8
"""
  Author:  Wusf --<wushifan221@gmail.com>
  Purpose: 
  Created: 2016/5/4
"""

import sqlite3 as lite
from datetime import datetime,timedelta

import Tools.GetLocalDatabasePath as GetPath
import Tools.LogOutputHandler as LogHandler
import Configs.RootPath as Root
RootPath = Root.RootPath