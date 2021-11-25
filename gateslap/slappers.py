from gateslap.myconnutils import QueryOneOff, QueryPersist, Database
from gateslap import mysql_config, pool_config, vslap_config
import time
import random

class Slapper(object):
    def __init__(self, sql_file):
        self.sql_file = sql_file
        self.min_time = int(vslap_config['sleep_min'])
        self.max_time = int(vslap_config['sleep_max'])
        self.process_file()

    def sleep_generator(self):
        print(str(self.min_time) + " all the way to " + str(self.max_time))
        sleeping=random.randint(self.min_time, self.max_time)/1000
        print("sleeping for " + str(sleeping) + "seconds.")
        time.sleep(sleeping)

    def db_connect(self):
        db = Database(mysql_config)
        return db

    def process_file(self):
        db_conn = self.db_connect()
        myfile = open(self.sql_file, "r")
        sql = myfile.readline()
        while sql:
            print(sql)
            #db.execute(sql)
            self.sleep_generator()
            sql = myfile.readline()

class OneSlapper(Slapper):
    def __init__(self, sql_file):
        super().__init__(sql_file)

    def db_connect(self):
        db = QueryOneOff(mysql_config)
        return db

class PersistentSlapper(OneSlapper):
    def __init__(self, sql_file):
        super().__init__(sql_file)

    def db_connect(self):
        db = QueryPersist(mysql_config, pool_config)
        return db
