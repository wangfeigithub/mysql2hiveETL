#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
import os
import subprocess
system = os.system("pwd")
print system



ret = os.popen('pwd').read()
print(ret)

getoutput =subprocess.getoutput('pwd')

print getoutput
