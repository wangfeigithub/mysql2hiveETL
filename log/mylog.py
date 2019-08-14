#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/27 8:51 PM
# @Desc    : 日志
# @File    : mylog.py
# @Software: PyCharm

import logging
import os
import time
import inspect


basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#日志配置
logging.basicConfig(level = logging.INFO,
                    format = '%(asctime)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt = '%Y-%m-%d %H:%M:%S',
                    filename = basedir +'/log/'+ time.strftime("%Y-%m-%d")+'.log',
                    filemode = 'a')


# 记录日志 info 信息
def info(info):
    logging.info('func:' + str(inspect.stack()[1][3]) + ' #' + info)
    #logging.info('呵呵')

# 记录错误信息
def error(info):
    logging.error('func:'+ inspect.stack()[1][3]+' #'+str(info))

if __name__ == '__main__':
    print('basedir:'+basedir)
    info('呵呵')

