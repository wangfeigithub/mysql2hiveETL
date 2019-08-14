#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# 日志模块






#日志配置

#日志配置
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt = '%Y-%m-%d %H:%M:%S')



logging.info(11)

logging.error("sisiis")
