#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# hive脚本

# 有变化的数据去重合并
INCREMENT_HIVE_SQL = '''
                    insert overwrite table {}.{}
                    select * from {}_tmp.{}_increment
                    union all 
                    select fu.* from {}.{} fu
                    left outer join {}_tmp.{}_increment inc on fu.{} = inc.{} 
                    where inc.{} is null;
                    '''
