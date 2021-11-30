# gateslap
Utility for generating traffic for VTGate.

Written as a python package, which will allow for more flexibility as
the program develops. The goal is to create a quick command that you can run
to start sending persistent and non-persistent MySQL connections.

## Requirements
Python can pip install the packages for you, however, if you want the
requirements to be managed by your OS you may want to install the dependencies
before doing the `python setup.py install` run_command

### Debian/Ubuntu
There are no packages available for dbutils so you will need to pip install
that package.

```
sudo apt-get update; sudo apt-get install python3-pymysql python3-tqdm
sudo pip3 install dbutils
```

### Redhat Linux/Fedora
You will need to enssure you have epel enabled to install these packages.

```
sudo yum -y install epel-release
sudo yum -y install python3-dbutils python3-PyMySQL python3-tqdm

```

## Install

```
git clone https://github.com/FancyFane/gateslap.git
cd gateslap
python setup.py install
```

## Usage
Currently this is designed to be ran with a `slapper.ini` in the current
directory, you can also provide the ini file as an argument on the command line.
```
gateslap
gateslap /path/to/slapper.ini
```

## TODO
* Allow for custom SQL files and tables
* Develop test files
* Expand sanity check when running


## Work In Progress
This utility is still a work in progres, if you are to install do so in a virtual environment.

```
git clone https://github.com/FancyFane/gateslap.git
cd gateslap
virtual venv
. venv/bin/activate
./reset.sh
```

The reset.sh script is a helper script to quickly clear things out and re-install the local copy
while development work continues. Once it runs you should be able to run the command anywhere while
in the virtual environment.

```
gateslap
````
