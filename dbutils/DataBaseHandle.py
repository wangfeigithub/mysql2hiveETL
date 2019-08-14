#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# mysql 通用工具类

import pymysql
import pandas as pd

#日志配置
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt = '%Y-%m-%d %H:%M:%S')

class DataBaseHandle(object):
    ''' 定义一个 MySQL 操作类'''


    def __init__(self,host,username,password,database,port):
        '''初始化数据库信息并创建数据库连接'''
        # 下面的赋值其实可以省略，connect 时 直接使用形参即可
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.db = pymysql.connect(self.host,self.username,self.password,self.database,self.port,charset='utf8',cursorclass=pymysql.cursors.DictCursor)



    #  这里 注释连接的方法，是为了 实例化对象时，就创建连接。不许要单独处理连接了。
    #
    # def connDataBase(self):
    #     ''' 数据库连接 '''
    #
    #     self.db = pymysql.connect(self.host,self.username,self.password,self.port,self.database)
    #
    #     # self.cursor = self.db.cursor()
    #
    #     return self.db


    def etlLog(self, source_database, source_table_name,exec_flag,msg,script):
        ''' 记录日志 '''
        sql = '''insert into t_pub_etl_log(etl_name,exec_flag,msg,script) values(%s, %s,%s, %s)'''
        args =(source_database+'.'+source_table_name,exec_flag,msg,script)


        self.cursor = self.db.cursor()

        try:
            # 执行sql
            self.cursor.execute(sql,args)
            # tt = self.cursor.execute(sql)  # 返回 插入数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except Exception as err:
            logging.error(err)
            # 发生错误时回滚
            self.db.rollback()
        finally:
            self.cursor.close()



    def insertDB(self,sql):
        ''' 插入数据库操作 '''

        try:
            # 执行sql
            self.cursor.execute(sql)
            # tt = self.cursor.execute(sql)  # 返回 插入数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except Exception as err:
            logging.error(err)
            self.db.rollback()
        finally:
            self.cursor.close()



    def deleteDB(self,sql):
        ''' 操作数据库数据删除 '''
        self.cursor = self.db.cursor()

        try:
            # 执行sql
            self.cursor.execute(sql)
            # tt = self.cursor.execute(sql) # 返回 删除数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except Exception as err:
            logging.error(err)
            # 发生错误时回滚
            self.db.rollback()
        finally:
            self.cursor.close()





    def updateDb(self,sql):
        ''' 更新数据库操作 '''

        self.cursor = self.db.cursor()

        try:
            # 执行sql
            self.cursor.execute(sql)
            # tt = self.cursor.execute(sql) # 返回 更新数据 条数 可以根据 返回值 判定处理结果
            # print(tt)
            self.db.commit()
        except Exception as err:
            logging.error(err)
            # 发生错误时回滚
            self.db.rollback()
        finally:
            self.cursor.close()



    def selectDb(self,sql):
        ''' 数据库查询 '''

        self.cursor = self.db.cursor()

        try:

            self.cursor.execute(sql) # 返回 查询数据 条数 可以根据 返回值 判定处理结果
            result = self.cursor.fetchall() # 返回所有记录列表


            return result
        except Exception as err:
            logging.error(err)
            print('Error: unable to fecth data')
        finally:
            self.cursor.close()

    def selectOneDb(self,sql):
        ''' 数据库查询一条 '''

        self.cursor = self.db.cursor()

        try:
            self.cursor.execute(sql) # 返回 查询数据 条数 可以根据 返回值 判定处理结果
            result = self.cursor.fetchone()# 返回所有记录列表
            return result
        except Exception as err:
            logging.error(err)
            print('Error: unable to fecth one data')
        finally:
            self.cursor.close()




    def closeDb(self):
        ''' 数据库连接关闭 '''
        self.db.close()





