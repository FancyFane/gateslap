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

    def __init__(self, mysql_config):
        self.host = mysql_config['host']
        self.user = mysql_config['user']
        self.password = mysql_config['password']
        self.db = mysql_config['database']
        self.port = int(mysql_config['port'])
        self.charset = mysql_config['charset']
        self.autocommit = mysql_config['autocommit']
        self.retry = bool(mysql_config['retry'])
        self.retry_time = int(mysql_config['retry_time'])
        self.exit_on_error = bool(mysql_config['exit_on_error'])
        self.drop_table = bool(mysql_config['drop_table'])


    def run_sql(self, sql):
        try:
            self.cur.execute(sql)
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
            errnum = e.args[0]
            errmsg = e.args[1]
            print("Error trying to run SQL:\n" + sql)
            if errnum == 1105:
                print("No healthy tablets error.")
                if self.retry == True:
                    print("Retry enabled, waiting " + str(self.retry_time) + \
                          "ms and trying SQL statment again.")
                    time.sleep(self.retry_time/1000)
                    self.cur.execute(sql)
                else:
                    print("Retry disabled, displaying the relevant " + \
                          "error msg and exiting:" + \
                          "\nError Number: " + str(errnum) + \
                          "\nError Message: " + errmsg)
                    if self.exit_on_error == True:
                        sys.exit(1)
                    pass
            elif errnum == 1050:
                table = ""
                if self.drop_table:
                    table=find_table(sql)
                    print("Duplicate table " + table + " found, dropping " + \
                          "per [mysql] configuration.")
                    self.cur.execute("DROP TABLE " + table + ";")
                    self.cur.execute(sql)
                else:
                    print("Not configured to drop tables. Manually drop " + \
                          "the " + table + " table, and rerun.")
                    if self.exit_on_error == True:
                        sys.exit(1)
            # Error not accounted for print out the error number and message
            else:
                print("\nError Number: " + str(errnum) + \
                      "\nError Message: " + errmsg)

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
    def __init__(self, mysql_config):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config)



# Using dbutils - PersistentDB for a dedicated connection per thread
# Docs: https://webwareforpython.github.io/DBUtils/main.html#modules
class QueryPersist(Database):

    def __init__(self, mysql_config, pool_config, purpose=""):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config)

        # Process additional attributes for the Connection Pool only need ping
        self.ping =  int(pool_config['ping'])
        self.persist_db = PersistentDB(creator=pymysql,
                                       host=self.host, port=self.port,
                                       user=self.user, password=self.password,
                                       database=self.db, charset=self.charset,
                                       threadlocal=threading.local,
                                       ping=self.ping)
        self.purpose = purpose
        # Establish connection upon creation
        self.connect()

    # override connection, as we want persistent conns not pymysql conns
    def connect(self):
        self.con = self.persist_db.steady_connection()
        self.cur = self.con.cursor()
        if self.purpose != "":
            print("Establishing persistent connection for " + self.purpose + ".")

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
        print("Closing persistent connection for " + self.purpose + ".")

# Using dbutils - PooledDB for pooled database connections
# NOTE: reset, must be set to false, or rollback is auto issued to mysql
# Docs: https://webwareforpython.github.io/DBUtils/main.html#modules
class QueryPooled(Database):
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
        			maxusage=self.maxshared, ping=self.ping, reset=False,
        			host=self.host, port=self.port, user=self.user,
        			password=self.password,database=self.db,charset=self.charset
        		)

    # The thread saftey is set to 1 for PyMySQL so each thread will need a
    # dedicated connection:
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
