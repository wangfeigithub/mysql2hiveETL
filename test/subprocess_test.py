#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei

import os
import sys
import subprocess

basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,basedir)


os_cmd_str = 'python ../hive/Mysql2Hive.py 1 2 3'

obj = subprocess.Popen(os_cmd_str,  shell=True, stderr=subprocess.PIPE)
# obj.stdin.write('print 1/0 \n')
# obj.stdin.write('print 2 \n')
# obj.stdin.write('print 3 \n')
# obj.stdin.write('print 4 \n')
# obj.stdin.close()
# print 99
# cmd_out = obj.stdout.read()
# obj.stdout.close()
cmd_error = obj.stderr.read()
obj.stderr.close()
print 99
print type(cmd_error)
print cmd_error
