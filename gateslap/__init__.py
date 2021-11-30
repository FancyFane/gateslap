import sys
from pathlib import Path
from gateslap.parser import ConfigFile
from gateslap.myconnutils import *

if len(sys.argv) > 1:
    CONFIGFILE=sys.argv[1]
else:
    CONFIGFILE="slapper.ini"

config_file = Path(CONFIGFILE)
try:
    my_abs_path = config_file.resolve(strict=True)
except FileNotFoundError:
    print("Config file " + CONFIGFILE + " not found.")
    sys.exit(1)

# TODO: Add more validation for config
mysql_config = ConfigFile(CONFIGFILE)["mysql"]
pool_config = ConfigFile(CONFIGFILE)["pool"]
gateslap_config = ConfigFile(CONFIGFILE)["gateslap"]
mysqlslap_config = ConfigFile(CONFIGFILE)["mysqlslap"]

# Due to the nature of pooled connections we must define
# our pool in the main package
try:
    db_pool=QueryPersist(mysql_config, pool_config)
except pymysql.err.OperationalError as e:
    errnum = e.args[0]
    if errnum == 2003:
        print("Unable to connect to database using connection details:")
        for val in mysql_config:
            print(val + ": " + mysql_config[val])
        sys.exit(1)
# Just a note we can not create a thread safe pymysql object
# this means it will need to be declared when it's needed.
# https://stackoverflow.com/questions/47163438/is-pymysql-connection-thread-safe-is-pymysql-cursor-thread-safe

# Create a variable to track threads, useful if an inturrupt is given
background_threads=[]
