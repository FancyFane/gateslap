#!/usr/bin/python3

from gateslap.myconnutils import QueryOneOff, QueryPooled
from gateslap.slappers import Slapper
from gateslap import *
from  gateslap.helpers import *
import sys, signal, time



# # TODO:   Add in SSL support for MySQL connection
#           Allow for Custom SQL files and Tables
#           Expand on sanity check CPU/Memory limits
#           Create Unit Test files

# Variables which help us in the "Ctrl + C" event
background_processes = []
created_tables = []

# Define a mysql connection for one off connections
# Used in testing connections and creating/dropping tables
mysql=QueryOneOff(mysql_config)

def start():
    sanity_check()
    create_table()
    sql_files=generate_sql()
    slap_vtgate(sql_files)
    drop_table_check()


def drop_table_check():
    '''Drop tables if configured to do so, at the end of the program
    running or when a CTRL + C event is detected'''
    if mysql_config['drop_table']:
        print("\nDropping generated tables per drop_tables.")
        for table in created_tables:
            print("Dropping table " + table + ".")
            mysql.execute('drop table ' + table + ';')
    sys.exit(0)


def sigint_handler(signal, frame):
    '''Close the program if user gives CTRL + C commands'''
    print ('CTRL + C detected cleaning up....')
    print("Sending kill to threads...")
    for t in background_processes:
        t.running = False
    time.sleep(int(gateslap_config['sleep_max'])/1000)
    drop_table_check()

signal.signal(signal.SIGINT, sigint_handler)


def sanity_check():
    '''Run basic test and exit if problems are detected.'''
    # TODO: Add in more basic checks here.
    if sys.version_info.major != 3 or sys.version_info.minor < 6:
        print("This program requires Python 3.6 or later.")
        sys.exit(1)

    # Test SQL statment
    sql_statment='SELECT 1 FROM dual;'

    # Test one off database connections
    try:
        results=mysql.fetch(sql_statment)
    except:
        print("Unable to connect to mysql. \n" +
              "Check '[mysql]' configurations in " + CONFIGFILE + ".")
        sys.exit(1)

    # Test persistent connections
    try:
        oneoff_sql=QueryPersist(mysql_config, pool_config)
        results=oneoff_sql.fetch(sql_statment)
    except:
        print("Unable to connect to mysql with persisting connection. \n" +
              "Check 'ping' in '[pool]' configurations in " + CONFIGFILE + ".")
        sys.exit(1)

    # Test pooled connections
    try:
        results=db_pool.fetch(sql_statment)
    except Exception as e:
        print("Unable to connect to mysql with a pooled connection.\n" +
              "Check '[pool]' configurations in " + CONFIGFILE + ".\n")
        print(e)
        sys.exit(1)


def create_table():
    '''There are two ways tables can be created, first is automatic mode,
    which uses mysqlslap to generate SQL statments, and second is using custom
    sql statments provided by 'create_table_sql' in [custom].'''

    mysqlslap="mysqlslap --only-print --number-int-cols=" + \
    mysqlslap_config['int_cols'] + " --number-char-cols=" + \
    mysqlslap_config['char_cols'] + " --number-of-queries=1 \
    --auto-generate-sql | awk '/CREATE TABLE/'"

    generate_create_sql=run_command(mysqlslap, shell=True)
    error=generate_create_sql[1].decode("utf-8")

    if error != "":
        print("An error has occured while generating the MySQL table " + \
              "creation sql:\n\n" + error + "\n\nCheck '[mysqlslap]' "+ \
              "configuration  " + CONFIGFILE + ".\n\n")
        sys.exit(1)

    sql=generate_create_sql[2].decode("utf-8")
    table=find_table(sql)
    auto_gen = (int(gateslap_config['pooled_conns']) +
                int(gateslap_config['oneoff_conns']) +
                int(gateslap_config['persist_conns']))
    # Count all connections if there's at least one create auto gen table
    if auto_gen > 0:
        mysql.execute(sql)
        created_tables.append(table)

    if custom_config['create_table_sql']:
        with open(custom_config['create_table_sql']) as file:
            for sql in file:
                try:
                    table=find_table(sql)
                except Exception as e:
                    print("No CREATE TABLE statment found in " + \
                          custom_config['create_table_sql'])
                if table in created_tables:
                    print("\nWARNING: The table " + table + " already exist." + \
                          "\nIt is suggested you modify your [custom] " + \
                          "create_table_sql file\n" + \
                          "to ensure there are no table conflicts." + \
                          "If you are using auto generate features t1 is " + \
                          "a reserved table name.")
                    if mysql_config['exit_on_error'] == True:
                        print("Exiting per configuration of exit_on_error.")
                        sys.exit(1)
                mysql.execute(sql)
                # It is possible to have a duplicate table name so check before
                # adding the new value
                if table not in created_tables:
                    created_tables.append(table)

def generate_sql():
    ''' There are two ways to define SQL files for threads, the first is Using
    mysqlslap, in which SQL files will be created in the directiory specified by
    tmp_dir in the [gateslap] config. Second, you can list out custom SQL files
    using pooled_sql, oneoff_sql, or persist_sql in the [custom] config.'''

    sql_files = {}

    def create_sql_file(cmd, file):
        run=run_command(cmd + " > " + file, shell=True)
        error=run[1].decode("utf-8")
        if error != "":
            print("An error has occured while generating synthetic SQL:\n\n" + \
            error + "\n\nCheck '[mysqlslap]' and '[gateslap]' " + \
            "configurations in " + CONFIGFILE + ". Also, make sure " +\
            "you have mysqlslap binaries installed.\n\n")
            sys.exit(1)

    tmp_dir=gateslap_config['tmp_dir']


    mysqlslap="mysqlslap " + \
    "--create-schema=" + mysql_config['database'] + \
    " --number-int-cols=" + mysqlslap_config['int_cols'] + \
    " --number-char-cols=" + mysqlslap_config['char_cols'] + \
    " --number-of-queries=" + mysqlslap_config['queries_per_process'] + \
    " --auto-generate-sql-load-type=" + mysqlslap_config['sql_type'] + \
    " --auto-generate-sql --no-drop --only-print " + \
    " --auto-generate-sql-unique-write-number=" + \
    mysqlslap_config['auto-generate-sql-unique-write-number'] + " | " + \
    "awk '!/CREATE SCHEMA|CREATE TABLE|use " + mysql_config['database'] + \
    "/' | sed '/^SELECT/ s/;$/ LIMIT 9990;/' | sort | uniq"

    sql_files.update({'pooled':[]})
    num_of_sql_files = int(gateslap_config['pooled_conns'])
    for file in range(num_of_sql_files):
        sql_file=tmp_dir + '/pooled_synthetic_sql_' + str(file+1) + '.sql'
        create_sql_file(mysqlslap, sql_file)
        sql_files['pooled'].append(sql_file)

    sql_files.update({'oneoff':[]})
    num_of_sql_files = int(gateslap_config['oneoff_conns'])
    for file in range(num_of_sql_files):
        sql_file=tmp_dir + '/oneoff_synthetic_sql_' + str(file+1) + '.sql'
        create_sql_file(mysqlslap, sql_file)
        sql_files['oneoff'].append(sql_file)

    sql_files.update({'persist':[]})
    num_of_sql_files = int(gateslap_config['persist_conns'])
    for file in range(num_of_sql_files):
        sql_file=tmp_dir + '/persist_synthetic_sql_' + str(file+1) + '.sql'
        create_sql_file(mysqlslap, sql_file)
        sql_files['persist'].append(sql_file)

    # TODO: Allow an override for custom SQL files here.
    # Config file custom_persist_sql_list=custom1.sql,custom2.sql,etc
    # Config file custom_oneoff_sql_list=customa.sql,customb.sql,etc

    return sql_files

def slap_vtgate(sql_files):
    '''There are three kinds of slappers, oneoff slappers, persistent slappers,
    and pooled slappers. These will be spun up per the dectionary passed to it,
    the dictionary should be defined by this point. exec() is used as this is
    being used to generate variable names for threads.'''

    for key, files in sql_files.items():
        for idx, file in enumerate(files):
            slapper_name=key + str(idx + 1)
            if key == 'pooled':
                exec(slapper_name + " = Slapper('" + file + "', 'pooled')")
            elif key == 'oneoff':
                exec(slapper_name + " = Slapper('" + file + "', 'oneoff')")
            elif key == 'persist':
                exec(slapper_name + " = Slapper('" + file + "', 'persist')")

            exec(slapper_name + ".start('" + slapper_name + "')")
            exec("background_processes.append(" + slapper_name + ")")

    for t in background_threads:
        t.join()
