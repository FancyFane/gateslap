#!/usr/bin/python3

from gateslap.myconnutils import QueryOneOff, QueryPersist
from gateslap.slappers import OneSlapper, PersistentSlapper
from gateslap import mysql_config, pool_config, vslap_config, mslap_config, CONFIGFILE
from  gateslap.helpers import *
import sys
from threading import Thread



# # TODO:   Create multi thread connections to the database
#           Allow for Custom SQL/Tables
#           Expand on sanity check CPU/Memory limits
#           Create Unit Test


def start():

    sanity_check()
    create_table()
    sql_files=generate_sql()
    slap_vtgate(sql_files)


def slap_vtgate(sql_files):

    for idx, file in enumerate(sql_files['persistent']):

        #### Stopping place is here
        #### Need to create a thread that calls Slapper Objects
        print(file)
        print(idx+1)
        newslapper = PersistentSlapper(file)
        pass


    for file in sql_files['oneoff']:
        newslapper = OneSlapper(file)


def generate_sql():

    # Initalize a dictionary of files to return
    sql_files = {}

    def create_sql_file(cmd, file):
        run=run_command(cmd + " > " + file, shell=True)
        error=run[1].decode("utf-8")
        if error != "":
            print("An error has occured while generating synthetic SQL:\n\n" + \
            error + "\n\nCheck '[mysqlslap]' and '[slappers]' " + \
            "configurations in " + CONFIGFILE + ".\n\n")
            sys.exit(1)

    tmp_dir=vslap_config['tmp_dir']

    mysqlslap="mysqlslap " + \
    "--create-schema=" + mysql_config['database'] + \
    " --number-int-cols=" + mslap_config['int_cols'] + \
    " --number-char-cols=" + mslap_config['char_cols'] + \
    " --number-of-queries=" + mslap_config['queries_per_process'] + \
    " --auto-generate-sql-load-type=" + mslap_config['sql_type'] + \
    " --auto-generate-sql --no-drop --only-print | " + \
    "awk '!/CREATE SCHEMA|CREATE TABLE/' |" + \
    "sed '/^SELECT/ s/;$/ LIMIT 9990;/'"

    sql_files.update({'persistent':[]})
    num_of_sql_files = int(vslap_config['persistent_conns'])
    for file in range(num_of_sql_files):
        sql_file=tmp_dir + '/persistent_synthetic_sql_' + str(file+1) + '.sql'
        create_sql_file(mysqlslap, sql_file)
        sql_files['persistent'].append(sql_file)

    sql_files.update({'oneoff':[]})
    num_of_sql_files = int(vslap_config['oneoff_conns'])
    for file in range(num_of_sql_files):
        sql_file=tmp_dir + '/oneoff_synthetic_sql_' + str(file+1) + '.sql'
        create_sql_file(mysqlslap, sql_file)
        sql_files['oneoff'].append(sql_file)


    # TODO: Allow an override for custom SQL files here.
    # Config file custom_persist_sql_list=custom1.sql,custom2.sql,etc
    # Config file custom_oneoff_sql_list=customa.sql,customb.sql,etc

    return sql_files

def create_table():
    mysqlslap="mysqlslap --only-print --number-int-cols=" + \
    mslap_config['int_cols'] + " --number-char-cols=" + \
    mslap_config['char_cols'] + " --number-of-queries=1 \
    --auto-generate-sql | awk '/CREATE TABLE/'"

    generate_create_sql=run_command(mysqlslap, shell=True)
    error=generate_create_sql[1].decode("utf-8")
    if error != "":
        print("An error has occured while generating the MySQL table " + \
              "creation sql:\n\n" + error + "\n\nCheck '[mysqlslap]' "+ \
              "configuration  " + CONFIGFILE + ".\n\n")
        sys.exit(1)
    mysql=QueryOneOff(mysql_config)

    # TODO: If custom SQL provided override 'create_sql' here

    create_sql=generate_create_sql[2].decode("utf-8")
    try:
        mysql.execute(create_sql)
    # Error occurs if table already exisit
    except Exception as error:
        if vslap_config['drop_table']:
            # TODO work with custom tables in the future t1 is safe to assume
            mysql.execute('drop table t1;')
            mysql.execute(create_sql)
        else:
            print("This MySQL table already exisit, drop the relevant table" + \
                  " and try running the application again.\n\n" + str(error) + \
                  "\n\n")
            sys.exit(1)

def sanity_check():
    if sys.version_info.major != 3 or sys.version_info.minor < 6:
        print("This program requires Python 3.6 or later.")
        sys.exit(1)

    # Single SQL querry per connection
    sql_statment='SELECT 1 FROM dual;'

    try:
        single_sql=QueryOneOff(mysql_config)
        results=single_sql.fetch(sql_statment)
    except:
        print("Unable to connect to mysql. \n" +
              "Check '[mysql]' configurations in " + CONFIGFILE + ".")
        sys.exit(1)

    # Test persistent connections
    try:
        multi_sql=QueryPersist(mysql_config, pool_config)
        multi_results=single_sql.fetch(sql_statment)
    except:
        print("Unable to connect to mysql pool.\n" +
              "Check '[pool]' configurations in " + CONFIGFILE + ".")
        sys.exit(1)
