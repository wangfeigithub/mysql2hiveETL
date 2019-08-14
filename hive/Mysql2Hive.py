#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# mysql更新到hive ETL 通用脚本



import os
import sys
import datetime

#日志配置
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt = '%Y-%m-%d %H:%M:%S')



basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,basedir)

from dbutils.DataBaseHandle import DataBaseHandle
from script.HiveScript import INCREMENT_HIVE_SQL
from script.SqoopScript import INCREMENT_ETL_SCRIPT, APPEND_ETL_SCRIPT, FULL_ETL_SCRIPT

from log.LogUtils import LogUtils
class Mysql2Hive(object):
    # 初始化
    def __init__(self):
        self.mysql_database = sys.argv[1]
        self.mysql_table = sys.argv[2]
        self.etl_date = sys.argv[3]


    def get_etl_info(self,ret):

        # 获取配置信息
        source_database = ret.get('source_database')
        source_table_name = ret.get('source_table_name')
        hive_database = ret.get('hive_database')
        hive_table = ret.get('hive_table')
        update_time_flag = ret.get('update_time_flag')
        create_time_flag = ret.get('create_time_flag')
        union_fields = ret.get('union_fields')
        host = ret.get('host')
        user = ret.get('user')
        passwd = ret.get('passwd')
        import_flag = ret.get('import_flag')
        columns_name = ret.get('columns_name')
        mappers = ret.get('mappers')
        queue = ret.get('queue')
        run_flag = ret.get('run_flag')

        return source_database,source_table_name,hive_database,hive_table,update_time_flag,create_time_flag,union_fields,host,user,passwd,import_flag,columns_name,mappers,queue,run_flag

    # 更新run_flag
    def update_run_flag(self, db_info,from_database,from_table):
        update_run_flag_sql = '''UPDATE t_hive_etl_config SET run_flag = 'init' WHERE source_database = '{}' and source_table_name ='{}' '''.format(from_database,from_table)
        db_info.updateDb(update_run_flag_sql)

    # 主方法
    def do(self):
        # 获取外部参数
        from_database = self.mysql_database
        from_table = self.mysql_table
        etl_date = self.etl_date

        # 获取需要etl的时间
        if etl_date == 'normal':
            v_last_date = str(datetime.date.today() - datetime.timedelta(days=1)) + ' 00:00:00'
            v_curr_date = str(datetime.date.today()) + ' 00:00:00'
        else:
            v_last_date = str(datetime.datetime.strptime(etl_date, '%Y-%m-%d'))
            v_curr_date = str(datetime.datetime.strptime(etl_date + '', '%Y-%m-%d') + datetime.timedelta(days=1))

        # 获取配置表连接 'hadoop101', 'user4bigdata', 'test', 'test', 3306
        db_info = DataBaseHandle('hadoop101', 'user4bigdata', 'test', 'test', 3306)

        # 获取配置信息
        TABLE_SQL = '''select * from t_hive_etl_config where source_database = '{}' and source_table_name = '{}' '''.format(
            from_database, from_table)
        table_result = db_info.selectOneDb(TABLE_SQL)
        if not table_result:
            msg = "ERROR : 读取配置表错误，无此配置信息"
            exec_flag = 2
            LogUtils.call_log(from_database, from_table, exec_flag, msg, TABLE_SQL)
            return

        source_database, source_table_name, hive_database, hive_table, update_time_flag, create_time_flag, union_fields, host, user, passwd, import_flag, columns_name, mappers, queue ,run_flag= self.get_etl_info(table_result)


        # sqoop开始导入
        if run_flag != 'new' and import_flag =='inc':

            logging.info('根据更新时间和创建时间增量导入')

            increment_etl_script = INCREMENT_ETL_SCRIPT.format(queue,host, source_database, user, passwd, source_table_name, columns_name, mappers, hive_database,hive_table,
                                   update_time_flag, v_last_date, update_time_flag, v_curr_date, create_time_flag, v_last_date, create_time_flag,v_curr_date)

            logging.info(increment_etl_script)

            flag_tmp_table = os.system(increment_etl_script)
            if flag_tmp_table != 0:
                msg = "ERROR : 根据更新时间和创建时间增量导入tmp表错误"
                exec_flag = 2
                LogUtils.call_log(from_database, from_table, exec_flag, msg, increment_etl_script)
                return

            increment_hive_sql = INCREMENT_HIVE_SQL.format(hive_database,hive_table,hive_database,hive_table,hive_database,hive_table,hive_database,hive_table,union_fields,union_fields,union_fields)

            logging.info(increment_hive_sql)

            flag_full_table = os.system('''hive -e "{}" '''.format(increment_hive_sql))
            if flag_full_table != 0:
                msg = "ERROR : 根据更新时间和创建时间增量导入full表错误"
                exec_flag = 2
                LogUtils.call_log(from_database, from_table, exec_flag, msg, increment_hive_sql)
                return

        elif run_flag != 'new' and import_flag =='append':
            logging.info('根据创建时间增量导入')

            append_etl_script = APPEND_ETL_SCRIPT.format(queue,host, source_database, user, passwd, source_table_name, columns_name, mappers,
                                hive_database,hive_table,create_time_flag, v_last_date, create_time_flag,v_curr_date)

            logging.info(append_etl_script)

            flag_append_table = os.system(append_etl_script)
            if flag_append_table != 0:
                msg = "ERROR : 根据创建时间增量导入full表错误"
                exec_flag = 2
                LogUtils.call_log(from_database, from_table, exec_flag, msg, append_etl_script)
                return

        elif run_flag == 'new' or import_flag =='full':
            logging.info('全量导入')

            full_etl_script = FULL_ETL_SCRIPT.format(queue,host, source_database, user, passwd, source_table_name, columns_name, mappers, hive_database,hive_table,create_time_flag,v_curr_date)

            logging.info(full_etl_script)

            flag_full_table = os.system(full_etl_script)
            if flag_full_table != 0:
                msg = "ERROR : 全量导入full表错误"
                exec_flag = 2
                LogUtils.call_log(from_database, from_table, exec_flag, msg, full_etl_script)
                return
            # 更新run_flag
            self.update_run_flag(db_info,from_database, from_table)

        else:
            logging.info( "ERROR : 请输入正确的 run_flag 和 import_flag 参数")

            msg = "ERROR : 请输入正确的 run_flag 和 import_flag 参数"
            exec_flag = 2
            LogUtils.call_log(from_database, from_table, exec_flag, msg, '参数不合法')
            return
        # 关闭连接
        db_info.closeDb()

if __name__ == '__main__':
    # 执行etl操作
    mysql2hive_etl = Mysql2Hive()
    mysql2hive_etl.do()
