#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# 执行 通用脚本

import os
import sys
import subprocess


basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,basedir)


from log.LogUtils import LogUtils


class ExecEtl(object):
    # 初始化
    def __init__(self):
        self.mysql_database = sys.argv[1]
        self.mysql_table = sys.argv[2]
        self.etl_date = sys.argv[3]

    # 主方法
    def do(self):
        # 获取外部参数
        from_database = self.mysql_database
        from_table = self.mysql_table
        etl_date = self.etl_date

        os_cmd_str = 'python /data/module/ETL/hive/Mysql2Hive.py {} {} {}'.format(from_database,from_table,etl_date)

        p = subprocess.Popen(os_cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (o, e) = p.communicate()

        # print(p.returncode)
        # print(o)

        msg = ""

        info_list = e.split("\n")

        for info in info_list:
            if 'ERROR' in info:
                msg += info
                msg += "\n"
        if len(msg)>0:
            exec_flag = 2
            LogUtils.call_log(from_database, from_table,exec_flag, msg, os_cmd_str)
            return
if __name__ == '__main__':
    # 执行etl操作
    exec_etl = ExecEtl()
    exec_etl.do()

