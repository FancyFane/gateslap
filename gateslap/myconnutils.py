import pymysql
from dbutils.pooled_db import PooledDB, SharedDBConnection

class Database(object):

# Using either connection type (persistent or non-persistent) we wil still
# need these basic connection details. As such we will make a Database super
# class to hold this info and define it once.

    def __init__(self, mysql_config):
        self.host = mysql_config['host']
        self.user = mysql_config['user']
        self.password = mysql_config['password']
        self.db = mysql_config['database']
        self.port = int(mysql_config['port'])
        self.charset = mysql_config['charset']
        self.autocommit = mysql_config['autocommit']

    def fetch(self, sql):
        self.__connect__()
        self.cur.execute(sql)
        result = self.cur.fetchall()
        self.__disconnect__()
        return result

    def execute(self, sql):
        self.__connect__()
        self.cur.execute(sql)
        self.__disconnect__()

    def __disconnect__(self):
        self.cur.close()
        self.con.close()

    def __connect__(self):
        self.con = pymysql.connect(host=self.host, user=self.user, password=self.password,
                                   db=self.db, port=self.port, cursorclass=pymysql.cursors.
                                   DictCursor)
        self.cur = self.con.cursor()

# Creating easy human readable object name to use in code
class QueryOneOff(Database):
    pass

# Using dbutils - PooledDB for persistent maxconnections
# PooledDB module selected from DBUtils as it has more flexibility on how
# connections are shared between threads. PersistentDB only has 1 to 1 mapping.
# Docs: https://webwareforpython.github.io/DBUtils/main.html#modules
class QueryPersist(Database):

    def __init__(self, mysql_config, pool_config):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config)

        # Process additional attributes for the Connection Pool
        self.maxconnections = int(pool_config['maxconnections'])
        self.mincached =  int(pool_config['mincached'])
        self.maxcached =  int(pool_config['maxcached'])
        self.maxshared =  int(pool_config['maxshared'])
        self.blocking =  pool_config['blocking']
        self.maxusage =  pool_config['maxusage']
        self.ping =  int(pool_config['ping'])
        self.pool = PooledDB(
        			creator=pymysql, maxconnections=self.maxconnections,
        			mincached=self.mincached, maxcached=self.maxcached,
        			maxshared=self.maxshared, blocking=self.blocking,
        			maxusage=self.maxshared, ping=self.ping,
        			host=self.host, port=self.port, user=self.user,
        			password=self.password,database=self.db,charset=self.charset
        		)

    # Override the way we connect
    def __connect__(self):
        self.con = self.pool.connection()
        self.cur = self.con.cursor()
