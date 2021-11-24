#!/usr/bin/python3

from gateslap.myconnutils import QueryOneOff, QueryPersist
from gateslap.parser import ConfigFile
import sys
import os

# # TODO: Create more complicated SQL statments (piggy back of mysqlslap)
# create multi thread connections to the database (both persistent and non-persistent)
# expand on sanity check (test db connection), CPU/Memory limits
# Find Place for code to live

def start():
    if len(sys.argv) > 1:
        CONFIGFILE=sys.argv[1]
    else:
        CONFIGFILE="mysql.ini"
    mysql_config = ConfigFile(CONFIGFILE)["mysql"]
    pool_config = ConfigFile(CONFIGFILE)["pool"]

    # After reading file check sanity
    sanity_check()

    # Single SQL querry per connection
    sql_statment='SELECT * FROM recipes;'
    single_sql=QueryOneOff(mysql_config)
    results=single_sql.fetch(sql_statment)
    print(results)

    # Run SQL using persistent connections
    multi_sql=QueryPersist(mysql_config, pool_config)
    multi_results=single_sql.fetch(sql_statment)
    print(multi_results)

def get_sql():
    # mysqlslap --create-schema=pokemon --only-print  --concurrency=5 --iterations=20 --number-int-cols=2 --number-char-cols=3 --auto-generate-sql --no-drop > test
    pass




def sanity_check():
    if sys.version_info.major != 3 or sys.version_info.minor < 6:
        print("This program requires Python 3.6 or later.")
        os._exit(1)
