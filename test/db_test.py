#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# mysql 通用工具类

import pymysql
import pandas as pd

from dbutils.DataBaseHandle import DataBaseHandle


class DBTest(object):
    ''' 定义一个 MySQL 操作类'''


    def __init__(self):
        pass


    def load(self,db_info):
        db = db_info.selectOneDb('select * from t_hive_etl_config')
        print db

        db_info.updateDb("UPDATE t_hive_etl_config SET mappers = '111' WHERE source_database = 'zkt' and source_table_name ='end_user' ")


    def do(self):
        DbHandle = DataBaseHandle('hadoop101', 'root', '123456', 'azkaban', 3306)
        self.load(DbHandle)



if __name__ == '__main__':



    # DbHandle.insertDB('insert into test(name) values ("%s")'%('FuHongXue'))
    # DbHandle.insertDB('insert into test(name) values ("%s")'%('FuHongXue'))
    # DbHandle.selectDb('select * from test')
    # DbHandle.updateDb('update test set name = "%s" where sid = "%d"' %('YeKai',22))
    # DbHandle.selectDb('select * from test')
    # DbHandle.insertDB('insert into test(name) values ("%s")'%('LiXunHuan'))
    # DbHandle.deleteDB('delete from test where sid > "%d"' %(25))
    #
    # result1 = DbHandle.selectOneDb('select * from t_hive_etl_config')
    # print result1

    dd  =DBTest()
    dd.do()
