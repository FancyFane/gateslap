from gateslap.myconnutils import QueryOneOff, QueryPooled, QueryPersist
from gateslap import (mysql_config, gateslap_config, pool_config,
background_threads, db_pool)
import re
import time
import random
import threading
from tqdm import tqdm

class Slapper(object):
    def __init__(self, sql_file, sql_type, thread_name=""):
        self.sql_file = sql_file
        self.min_time = int(gateslap_config['sleep_min'])
        self.max_time = int(gateslap_config['sleep_max'])
        self.timer_on = bool(gateslap_config['sleep_between_query'])
        self.running = True
        self.thread_name = thread_name
        self.sql_type = sql_type
        self.file_len()
        self.db_conn()

    def db_conn(self):
        if self.sql_type == "pooled":
            self.db = db_pool
        elif self.sql_type == "oneoff":
            self.db = QueryOneOff(mysql_config)
        elif self.sql_type == "persist":
            self.db = QueryPersist(mysql_config, pool_config)

    def running():
        doc = "The running property."
        def fget(self):
            return self._running
        def fset(self, value):
            self._running = value
        def fdel(self):
            del self._running
        return locals()
    running = property(**running())

    def timer_on():
        doc = "The timer_on property."
        def fget(self):
            return self._timer_on
        def fset(self, value):
            self._timer_on = value
        def fdel(self):
            del self._timer_on
        return locals()
    timer_on = property(**timer_on())

    def file_len(self):
        with open(self.sql_file) as f:
            for i, l in enumerate(f):
                pass
        self.length = i + 1

    def sleep_generator(self):
        sleeping=random.randint(self.min_time, self.max_time)/1000
        time.sleep(sleeping)

    def process_file(self):
        # Create a new progress bar
        bar = tqdm(total=self.length, desc=self.thread_name)

        # Open the given SQL file
        with open(self.sql_file) as file:
            for sql in file:

                # Create a way to kill the loop if Ctrl + C given
                if self.running != True:
                    bar.close()
                    break

                # Execute the SQL statment
                if ";" in sql:
                    self.db.execute(sql)

                # Increment the bar by a value of 1
                bar.update(1)

                # Sleep for a random amount of time if enabled
                if self.timer_on:
                    self.sleep_generator()

            # Close the generated progress bar
        bar.close()

    def close(self):
        bar.close()

    def start(self, threadName):
        # Start a new thread
        self.thread_name=threadName
        thread = threading.Thread(target=self.process_file,
                                  name=self.thread_name,
                                  daemon=True)
        thread.start()
        background_threads.append(thread)
