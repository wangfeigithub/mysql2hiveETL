import pymysql


class db(object):
	__db = ""
	__cursor = ""
	seleTable = ''  ##要执行操作的表名
	strWhere = ''  ##where 条件
	strColumns = "*"  ##查询的字段
	__last_sql = " "  ##执行的sql

	def __init__(self):
		try:
			db = pymysql.connect(host="", user="", password="",
								 database="", port="", cursorclass=pymysql.cursors.DictCursor)
			self.__db = db
			# 使用 cursor() 方法创建一个游标对象 cursor
			self.__cursor = self.__db.cursor()
		except pymysql.MySQLError as e:
			print(e)
			exit()
		pass

	# def table(self, table):
	# 	self.seleTable = databases.prefix + table;
	# 	pass

	def where(self, key='', operator="=", value=""):
		if operator in ['=', '>', '<', '>=', '<=', '!=', '<>', 'like']:
			if self.strWhere: self.strWhere += " AND "  ##如果前方有值的话 则and
			self.strWhere += (("%s %s '%s'" % (key, operator, value)))
			pass

	def orWhere(self, key='', operator="=", value=""):
		if operator in ['=', '>', '<', '>=', '<=', '!=', '<>', 'like']:
			if self.strWhere: self.strWhere += " OR "
			self.strWhere += (" OR %s %s '%s'" % (key, operator, value))
			pass

	def first(self, columns="*"):
		self.strColumns = columns
		pass

	##查询一条数据
	def find(self):
		where = ""
		if self.strWhere: where = " WHERE " + self.strWhere
		sql = """SELECT %s FROM %s %s LIMIT 1 """ % (self.strColumns, self.seleTable, where)
		self.execute(sql=sql)
		return self.__cursor.fetchone()
		pass

	##查询一条数据
	def get(self):
		where = ""
		if self.strWhere: where = " WHERE " + self.strWhere
		sql = """SELECT %s FROM %s %s """ % (self.strColumns, self.seleTable, where)
		return self.execute(sql=sql).fetchall()
		pass

	##执行插入操作
	##返回最后一条id
	def add(self, data={}):
		if (self.seleTable):
			sqlKey = []
			sqlValue = []
			for key in data:
				sqlKey.append(key)
				sqlValue.append(data[key])
				pass
			sql = """INSERT INTO %s (`%s`) VALUES (%s)""" % (self.seleTable, "`,`".join(sqlKey),str(sqlValue).strip('[').strip(']'))
			self.execute(sql=sql)
			return self.__cursor.lastrowid
		else:
			return "No select tables"

	##更新语句
	def update(self, data={}):
		if (self.seleTable):
			attrs = ""
			for k,v in data.items():
				if isinstance(v,str):
					v = "\'" + v + "\'"
				attrs += "`%s` = %s ,"%(k,v)
			where = ""
			if self.strWhere: where = " WHERE " + self.strWhere
			sql = """UPDATE %s SET %s %s """ % (self.seleTable,attrs[:-1] , where)
			self.execute(sql=sql)
			return self.__cursor.lastrowid
		else:
			return "No select tables"
		pass

	##get LaseSql
	def LastSql(self):
		return self.__last_sql

	##sql执行
	def execute(self, sql):
		self.__last_sql = str(sql)
		try:
			self.__cursor.execute(sql)
			# 提交到数据库执行
			self.__db.commit()
			self.close()
		except pymysql.MySQLError as error:
			##数据回滚操作
			self.__db.rollback()
			self.close()
			print(error)
		pass

	def close(self):
		if (self.__db):
			self.strWhere = ""
			self.strColumns = ""
			if (type(self.__cursor) == 'object'):
				self.__cursor.close()  # 关闭标记位
			if (type(self.__db) == 'object'):
				self.__db.close()
