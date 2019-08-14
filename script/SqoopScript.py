#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# hive脚本


# 有变化的数据去重合并
INCREMENT_ETL_SCRIPT = '''
         /data/module/sqoop/bin/sqoop import -Dmapred.job.queue.name={}\
         --connect jdbc:mysql://{}:3306/{}?tinyInt1isBit=false \
         --username {} \
         --password {} \
         --table {}  \
         --columns "{}" \
         --num-mappers {} \
         --hive-import  \
         --hive-overwrite \
         --hive-database {}_tmp  \
         --hive-table {}_increment \
         --direct \
         --delete-target-dir \
         --where "({} >='{}' and {} <'{}') or ( {} >='{}' and {} <'{}' )" 
        '''



# 按照创建时间追加
APPEND_ETL_SCRIPT = '''
         /data/module/sqoop/bin/sqoop import -Dmapred.job.queue.name={}\
         --connect jdbc:mysql://{}:3306/{}?tinyInt1isBit=false \
         --username {} \
         --password {} \
         --table {}  \
         --columns "{}" \
         --num-mappers {} \
         --hive-import  \
         --hive-database {}  \
         --hive-table {} \
         --direct \
         --delete-target-dir \
         --where "{} >='{}' and {} <'{}'" 
        '''
# 全量导入
FULL_ETL_SCRIPT = '''
         /data/module/sqoop/bin/sqoop import -Dmapred.job.queue.name={}\
         --connect jdbc:mysql://{}:3306/{}?tinyInt1isBit=false \
         --username {} \
         --password {} \
         --table {}  \
         --columns "{}" \
         --num-mappers {} \
         --hive-import  \
         --hive-overwrite \
         --hive-database {} \
         --hive-table {} \
         --direct \
         --delete-target-dir \
         --where " {} <'{}'"
        '''
