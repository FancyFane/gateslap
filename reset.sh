#!/bin/bash

rm -rf /tmp/*.sql
rm -rf venv/lib/python3.8/site-packages/{progress,pymysql,PyMySQL}*
rm -f venv/bin/helena

pip install --editable . > /dev/null 2>&1

gateslap
