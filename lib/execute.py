#!/usr/bin/env python

"""
execute.py

Execute commands.

Copyright (C) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import subprocess
import signal
import logging


logger = logging.getLogger(__name__)


class Signal(Exception):
    """
    This exception is raise by the signal handler.
    """
    pass


class Timeout(Exception):
    """
    This exception is raised when the command exceeds the defined timeout
    duration and the command is killed.
    """
    def __init__(self, cmd, timeout):
        self.cmd = cmd
        self.timeout = timeout

    def __str__(self):
        return "Command '%s' timed out after %d second(s)." % \
               (self.cmd, self.timeout)


class Retcode(Exception):
    """
    This exception is raise when a command exits with a non-zero exit status.
    """
    def __init__(self, cmd, retcode, output=None):
        self.cmd = cmd
        self.retcode = retcode
        self.output = output

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d" % \
               (self.cmd, self.retcode)


def alarm_handler(signum, frame):
    raise Signal


def execute(cmd, timeout=None):
    """
    Execute a command in the default shell. If a timeout is defined the command
    will be killed if the timeout is exceeded and an exception will be raised.

    Inputs:
        cmd     (str): Command to execute
        timeout (int): Command timeout in seconds
    Outputs:
        output (str): STDOUT/STDERR
    """
    logger.debug(cmd)

    # Define the timeout signal
    if timeout:
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(timeout)

    try:
        # Execute the command and wait for the subprocess to terminate
        # STDERR is redirected to STDOUT
        phandle = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        # Read the stdout/sterr buffers and retcode
        output, _ = phandle.communicate()
        retcode = phandle.poll()
    except Signal:
        # Kill the running process
        phandle.kill()
        raise Timeout(cmd=cmd, timeout=timeout)
    except:
        logger.debug("Unhandled exception", exc_info=True)
        raise
    else:
        # Possible race condition where alarm isn't disabled in time
        signal.alarm(0)

    # Raise an exception if the command exited with non-zero exit status
    if retcode:
        raise Retcode(cmd, retcode, output=output)

    logger.debug(output)

    return output


def execute_nmc(cmd, timeout=None):
    """
    Execute a command in NMC. If a timeout is defined the command
    will be killed when the timeout is exceeded.

    Inputs:
        cmd     (str): NMC command to execute
        timeout (int): Command timeout in seconds
    Outputs:
        retcode  (int): Return code
        output  (list): STDOUT/STDERR
    """
    nmc = "nmc -c \"%s\"" % cmd

    try:
        output = execute(nmc, timeout)
    except:
        raise

    return output


def execute_ssh(cmd, host, timeout=None):
    """
    Execute a command remotely. If a timeout is defined the command
    will be killed when the timeout is exceeded.

    Inputs:
        cmd     (str): NMC command to execute
        host    (str): Remote hostname or IP
        timeout (int): Command timeout in seconds
    Outputs:
        retcode  (int): Return code
        output  (list): STDOUT/STDERR
    """
    ssh = "ssh %s \"%s\"" % (host, cmd)

    try:
        output = execute(ssh, timeout)
    except:
        raise

    return output
