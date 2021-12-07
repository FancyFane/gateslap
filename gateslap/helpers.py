import subprocess, re

def find_table(create_sql):
    '''Given a create SQL statment string, return the table name'''
    search = re.search('(?<=CREATE TABLE )[^ ]*', create_sql, re.IGNORECASE)
    table = search.group(0)
    return table

def run_background_command(command):
    '''Run a command as a subprocess and place in the background
       returns PID of spawned process
       Assumes NO input from stdin at all
       stderr/stdout are piped to /dev/null'''
    proc = subprocess.Popen(command.split(), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
    return proc.pid

def run_command(command, timeout=None, shell=False, path=None):
    '''Runs a command as a subprocess
       Returns: exitcode, stderr, stdout
       stderr and stdout are bytestreams, potentially with escape codes.
       command - string for command to run
       timeout - number of seconds to wait while running command
       shell - False: launch process directly;  True: launch in a subshell
       path - Prepend PATH variable in subprocess environment with provided
              path
       On timeout, raises subprocess.TimeoutExpired
       Assumes NO input from stdin at all'''
    if path:
        env = os.environ.copy()
        env["PATH"] =  path + ":" + env["PATH"]
    if shell:
        try:
            result = subprocess.run(command, capture_output=True, check=False,
                timeout=timeout, input="", shell=True, env=env if path else None)
        except TypeError:
            result = subprocess.run(command, stderr=subprocess.PIPE,
                stdout=subprocess.PIPE, check=False, timeout=timeout,
                input="", shell=True, env=env if path else None)

    else:
        try:
            result = subprocess.run(command.split(), capture_output=True,
                check=False, timeout=timeout, input="", env=env if path else None)
        except TypeError:
            result = subprocess.run(command.split(), stderr=subprocess.PIPE,
                stdout=subprocess.PIPE, check=False, timeout=timeout,
                input="", env=env if path else None)
    return result.returncode, result.stderr, result.stdout
