#!/bin/bash

rm -rf venv/lib/python3.8/site-packages/{click,pymysql,PyMySQL}*
rm -f venv/bin/helena

pip install --editable .

gateslap
