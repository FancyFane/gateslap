import pymysql
from dbutils.pooled_db import PooledDB, SharedDBConnection
from dbutils.persistent_db import PersistentDB
from gateslap.helpers import *
import time, sys, threading

class Database(object):

    ''' Using any db connection type (persistent or non-persistent) we wil still
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
        self.read_timeout = int(mysql_config['read_timeout'])
        self.write_timeout = int(mysql_config['write_timeout'])

        try:
            self.retry_count = int(errors_config['retry_count'])
            if self.retry_count < 1:
                self.retry_count = 0
        except:
            self.retry_count = 0


    def run_sql(self, sql):
        '''Run SQL and do some basic error handling for demonstration purposes
        if errors are handled and the application is configured for error handl-
        ing then we will pass off to retry_sql() funciton.'''
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
        '''Here we will implement error handling where we retry the sql statment
        until we get success. We are implementing a oneoff connection for this
        retry as it helps resolve issues with pooled connections. We retry until
        we hit our retry_count value if we do not resolve it print the error.'''
        if self.retry_count == 0:
            sys.exit(1)
        for retry_attempt in range(1, self.retry_count+1):
            print("Retry attempt " + str(retry_attempt) + " out of " + \
                  str(self.retry_count) + " sleeping for " + \
                  str(self.retry_time) + "ms and trying again.")
            time.sleep(self.retry_time/1000)
            try:
                self.reconnect()
                self.retry_cur.execute(sql)
                self.retry_con.close()
                self.retry_cur.close()
            except Exception as e:
                if retry_attempt == self.retry_count:
                    print("Unable to resolve error, shutting down. \n" + str(e))
                    self.cur.close()
                    self.con.close()
                    sys.exit(1)
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
                               DictCursor,
                               read_timeout=self.read_timeout,
                               write_timeout=self.write_timeout)
        self.cur = self.con.cursor()

    def reconnect(self):
        '''Do NOT overide this function as it allows for oneoff connections
        used to resolve errors that may popup in the retry_sql() function.'''
        self.retry_con = pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.password,
                               db=self.db,
                               port=self.port,
                               cursorclass=pymysql.cursors.
                               DictCursor,
                               read_timeout=self.read_timeout,
                               write_timeout=self.write_timeout)
        self.retry_cur = self.retry_con.cursor()



class QueryOneOff(Database):
    ''' This class is just a glamor name; as it is a copy of Database. '''
    def __init__(self, mysql_config, errors_config):
        # Ensure we process the mysql_config using the super class
        super().__init__(mysql_config, errors_config)



class QueryPersist(Database):
    '''Using dbutils - PersistentDB for a dedicated connection per thread
    Docs: https://webwareforpython.github.io/DBUtils/main.html#modules we will
    copy details from Database and extend the calss. '''

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



class QueryPooled(Database):

    ''' Using dbutils - PooledDB for pooled database connections
    NOTE: reset, must be set to false, or rollback is auto issued to mysql
    Docs: https://webwareforpython.github.io/DBUtils/main.html#modules we will
    copy details from Database and extend the calss. '''

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
        			maxusage=self.maxshared, ping=self.ping,
        			host=self.host, port=self.port, user=self.user, reset=False,
        			password=self.password,database=self.db,charset=self.charset
        		)

    def execute(self, sql):
        with self.pool.connection(shareable=False) as self.con:
            with self.con.cursor() as self.cur:
                self.run_sql(sql)


    def fetch(self, sql):
        with self.pool.connection() as self.con:
            with self.con.cursor() as self.cur:
                self.run_sql(sql)
                result = self.cur.fetchall()
        return result
