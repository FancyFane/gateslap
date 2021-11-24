# gateslap
Utility for generating traffic for VTGate

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

## TODO
* Generate test SQL using mysqlslap
* Create threads to generate traffic
* Create Sanity Checks
