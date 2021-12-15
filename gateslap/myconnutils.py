import pymysql
from dbutils.pooled_db import PooledDB, SharedDBConnection
from dbutils.persistent_db import PersistentDB
from gateslap.helpers import *
import time, sys, threading

class Database(object):

    ''' Using either connection type (persistent or non-persistent) we wil still
    need these basic connection details. As such we will make a Database super
    class to hold this info and define it once.'''

# TODO add in SSL support here

    def __init__(self, mysql_config, errors_config):
        self.host = mysql_config['host']
        self.user = mysql_config['user']
        self.password = mysql_config['password']
        self.db = mysql_config['database']
        self.port = int(mysql_config['port'])
        self.charset = mysql_config['charset']
        self.autocommit = mysql_config['autocommit']
        self.retry_time = int(errors_config['retry_time'])
        self.drop_table = bool(errors_config['drop_table'])

        try:
            self.retry_count = int(errors_config['retry_count'])
            if self.retry_count < 1:
                self.retry_count = 0
        except:
            self.retry_count = 0


    def run_sql(self, sql):
        try:
            self.cur.execute(sql)
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
            errnum = e.args[0]
            errmsg = e.args[1]
            print("\nError Number: " + str(errnum) + \
                  "\nError Message: " + errmsg + \
                  "\nSQL Statment: " + sql)
            if errnum == 1105:
                # No primary found, likely a reparent event
                self.retry_sql(sql)
            elif errnum == 1050:
                # Trying to create a table that is already created
                # if drop_table is set then drop and recreate the table
                table = ""
                if self.drop_table:
                    table=find_table(sql)
                    self.cur.execute("DROP TABLE " + table + ";")
                    self.cur.execute(sql)
                else:
                    # drop_table not set, manual intervention needed; bail
                    print("Not configured to drop tables. Manually drop " + \
                          "the " + table + " table, and rerun.")
                    sys.exit(1)
            else:
                self.retry_sql(sql)


    def retry_sql(self, sql):
        if self.retry_count == 0:
            sys.exit(0)
        for retry_attempt in range(1, self.retry_count+1):
            print("Retry attempt " + str(retry_attempt) + " out of " + \
                  str(self.retry_count) + " sleeping for " + \
                  str(self.retry_time) + "ms and trying again.")
            time.sleep(self.retry_time/1000)
            try:
                self.cur.execute(sql)
            except Exception as e:
                if retry_attempt == self.retry_count:
                    print("Unable to resolve error, shutting down." + str(e))
                    sys.exit(0)
            else:
                break


    def fetch(self, sql):
        self.connect()
        self.run_sql(sql)
        result = self.cur.fetchall()
        self.disconnect()
        return result

    def execute(self, sql):
        self.connect()
        self.run_sql(sql)
        self.disconnect()

    def disconnect(self):
        self.cur.close()
        self.con.close()

    def connect(self):
        self.con = pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.password,
                               db=self.db,
                               port=self.port,
                               cursorclass=pymysql.cursors.
                               DictCursor)
        self.cur = self.con.cursor()



# Creating easy human readable object name to use in code, this is the same
# as using the Database Object.
class QueryOneOff(Database):
    def __init__(self, mysql_config, errors_config):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config, errors_config)



# Using dbutils - PersistentDB for a dedicated connection per thread
# Docs: https://webwareforpython.github.io/DBUtils/main.html#modules
class QueryPersist(Database):

    def __init__(self, mysql_config, pool_config, errors_config):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config, errors_config)

        # Process additional attributes for the Connection Pool only need ping
        self.ping =  int(pool_config['ping'])
        self.persist_db = PersistentDB(creator=pymysql,
                                       host=self.host, port=self.port,
                                       user=self.user, password=self.password,
                                       database=self.db, charset=self.charset,
                                       threadlocal=threading.local,
                                       ping=self.ping)
        # Establish connection upon creation
        self.connect()

    # override connection, as we want persistent conns not pymysql conns
    def connect(self):
        self.con = self.persist_db.steady_connection()
        self.cur = self.con.cursor()


    # override execute/fetch to only manipulate the cursor and not terminate
    # the persistent connections
    def execute(self, sql):
        self.run_sql(sql)

    def fetch(self, sql):
        self.run_sql(sql)
        result = self.cur.fetchall()
        return result

    def disconnect(self):
        self.cur.close()
        self.con.close()

# Using dbutils - PooledDB for pooled database connections
# NOTE: reset, must be set to false, or rollback is auto issued to mysql
# Docs: https://webwareforpython.github.io/DBUtils/main.html#modules
class QueryPooled(Database):
    def __init__(self, mysql_config, pool_config, errors_config):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config, errors_config)

        # Process additional attributes for the Connection Pool
        self.maxconnections = int(pool_config['maxconnections'])
        self.mincached =  int(pool_config['mincached'])
        self.maxcached =  int(pool_config['maxcached'])
        self.maxshared =  int(pool_config['maxshared'])
        self.blocking =  pool_config['blocking']
        self.maxusage =  pool_config['maxusage']
        self.ping =  int(pool_config['ping'])
        # reset=False must be set or SQL rollbacks will be issued
        self.pool = PooledDB(
        			creator=pymysql, maxconnections=self.maxconnections,
        			mincached=self.mincached, maxcached=self.maxcached,
        			maxshared=self.maxshared, blocking=self.blocking,
        			maxusage=self.maxshared, ping=self.ping, read_timeout=10,
        			host=self.host, port=self.port, user=self.user, reset=False,
        			password=self.password,database=self.db,charset=self.charset
        		)

    # The thread saftey is set to 1 for PyMySQL so each thread will need a
    # dedicated connection :
    # https://www.python.org/dev/peps/pep-0249/
    # https://github.com/PyMySQL/PyMySQL/blob/main/pymysql/__init__.py#L55
    def execute(self, sql):
        with self.pool.connection() as self.con:
            with self.con.cursor() as self.cur:
                self.run_sql(sql)


    def fetch(self, sql):
        with self.pool.connection() as self.con:
            with self.con.cursor() as self.cur:
                self.run_sql(sql)
                result = self.cur.fetchall()
        return result
