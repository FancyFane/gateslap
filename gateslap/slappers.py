from gateslap.myconnutils import QueryOneOff, QueryPersist, Database
from gateslap import mysql_config, pool_config, gateslap_config, background_threads, screen_position
import time
import random
import threading
from tqdm import tqdm

class Slapper(object):
    def __init__(self, sql_file):
        self.sql_file = sql_file
        self.min_time = int(gateslap_config['sleep_min'])
        self.max_time = int(gateslap_config['sleep_max'])
        self.timer_on = bool(gateslap_config['sleep_between_query'])
        self.running = True
        self.thread_name = ""
        self.file_len()

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

    def file_len(self):
        with open(self.sql_file) as f:
            for i, l in enumerate(f):
                pass
        self.length = i + 1

    def sleep_generator(self):
        sleeping=random.randint(self.min_time, self.max_time)/1000
        time.sleep(sleeping)

    def db_connect(self):
        db = Database(mysql_config)
        return db

    def process_file(self):
        bar = tqdm(total=self.length, position=screen_position, desc='thread' + self.thread_name)
        db_conn = self.db_connect()
        with open(self.sql_file) as file:
            for sql in file:
                if self.running != True:
                    break
                db_conn.execute(sql)
                bar.update(1)
                if self.timer_on:
                    self.sleep_generator()
            bar.close()

    def start(self, threadName):
        self.thread_name=threadName
        thread = threading.Thread(target=self.process_file,
                                  name=self.thread_name,
                                  daemon=True)
        thread.start()
        background_threads.append(thread)
        global screen_position
        screen_position += 1


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
