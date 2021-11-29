import sys
from pathlib import Path
from gateslap.parser import ConfigFile


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
gateslap_config = ConfigFile(CONFIGFILE)["slappers"]
mysqlslap_config = ConfigFile(CONFIGFILE)["mysqlslap"]

# Create a threads variable to help update.
background_threads=[]
screen_position=0
