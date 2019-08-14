#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 下午5:16
# @Author  : wangfei
# 配置表信息

# 配置表
CREATE TABLE `t_hive_etl_config` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `source_database` varchar(32) DEFAULT NULL COMMENT '源库',
  `source_table_name` varchar(128) NOT NULL COMMENT '源表',
  `hive_database` varchar(128) NOT NULL COMMENT 'hive库',
  `hive_table` varchar(128) NOT NULL COMMENT 'hive表',
  `update_time_flag` varchar(128) NOT NULL COMMENT '更新字段',
  `create_time_flag` varchar(128) NOT NULL COMMENT '创建字段',
  `union_fields` varchar(128) NOT NULL COMMENT '关联字段',
  `host` varchar(128) DEFAULT NULL COMMENT 'host',
  `user` varchar(64) DEFAULT NULL COMMENT '登陆名',
  `passwd` varchar(64) DEFAULT NULL COMMENT '登陆密码',
  `import_flag` varchar(32) DEFAULT NULL COMMENT '导入标志 inc:更新字段和创建字段导入  append:创建时间导入 full:全量导入',
  `columns_name` varchar(1536) DEFAULT NULL COMMENT '需要导入的字段',
  `mappers` tinyint(4) DEFAULT '5' COMMENT '用几个mapper',
  `queue` varchar(64) DEFAULT 'default' COMMENT 'yarn队列名称',
  `run_flag` varchar(32) DEFAULT 'new' COMMENT '是否初始化 new:未初始化,init:初始化',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录生成时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

# 日志表
CREATE TABLE `t_pub_etl_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `etl_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'etl过程名称',
  `start_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `exec_flag` tinyint(4) NOT NULL DEFAULT '1' COMMENT '执行状态1 表示成功 2表示失败',
  `msg` text NOT NULL COMMENT '错误信息',
  `script` text NOT NULL COMMENT '执行的语句',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COMMENT='etl过程日志表';
