#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# mysql更新到hive ETL 通用脚本



import os
import sys
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,basedir)

from dbutils.DataBaseHandle import DataBaseHandle


class LogUtils(object):

    @staticmethod
    def call_log(from_database, from_table, exec_flag, msg, TABLE_SQL):
        db_info = DataBaseHandle('hadoop101', 'user4bigdata', 'test', 'test', 3306)
        db_info.etlLog(from_database, from_table, exec_flag, msg, TABLE_SQL)
        db_info.closeDb()





