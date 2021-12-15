# gateslap
Utility for generating traffic for [Vitess](https://github.com/vitessio/vitess) VTGate.

There are several knobs available in the slapper.ini file the purpose
of this utility is to generate light VTGate traffic for testing. Synthetic
SQL files are generated using mysqlslap utility; this is how the name 
`gateslap` came to be. Alternatively, you can create your own SQL files
for gateslap to run. 


## Requirements
Python can pip install the needed packages, however, the os may also
install some of these packages and have the dependencies managed by your
OS. Aside from the python packages there is also a requirement to have
`mysqlslap` installed which typically comes with the `mysql-server` package. 


### Debian/Ubuntu
There is no package avilable for `dbutils`. We will leave this to the setup.py
script to install the python package. 
```
sudo apt-get update; sudo apt-get install python3-pymysql python3-tqdm mysql-server
```

### Redhat Linux/Fedora
You will need to enssure you have epel enabled to install these packages.
```
sudo yum -y install epel-release
sudo yum -y install python3-dbutils python3-PyMySQL python3-tqdm mysql-community-server
```

## Install instructions
```
git clone https://github.com/FancyFane/gateslap.git
cd gateslap
virtual venv
. venv/bin/activate
sudo python3 setup.py install
```

## Usage
Currently this is designed to be ran with a `slapper.ini` in the current
directory, you can also provide the ini file as an argument on the command line.
```
gateslap
gateslap /path/to/slapper.ini
```

There are additional .ini files in the examples folder.


## TODO
* Allow for SSL encryption
* Develop test files
* Expand sanity (resorce) check when running
