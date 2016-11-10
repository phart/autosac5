"""
execute.py

Execute commands.

Copyright (C) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import sys
import subprocess
import signal
import logging


logger = logging.getLogger(__name__)


class _Signal(Exception):
    """
    This exception is raise by the signal handler.
    """
    pass


class TimeoutError(Exception):
    """
    This exception is raised when the command exceeds the defined timeout
    duration.

    Attributes:
        cmd (str): Command
        timeout (int): Timeout duration
    """

    def __init__(self, cmd, timeout):
        self.cmd = cmd
        self.timeout = timeout

    def __str__(self):
        return "Command '%s' timed out after %d second(s)." % \
               (self.cmd, self.timeout)


class RetcodeError(Exception):
    """
    This exception is raise when a command exits with a non-zero exit status.

    Attributes:
        cmd (str): Command
        retcode (int): Command return code
        output (str): stderr/stdout
    """

    def __init__(self, cmd, retcode, output=None):
        self.cmd = cmd
        self.retcode = retcode
        self.output = output

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d" % \
               (self.cmd, self.retcode)


def alarm_handler(signum, frame):
    """
    We only get here if the timeout is exceeded.
    """
    raise _Signal


def execute(cmd, timeout=None):
    """
    Execute a command in the default shell. If a timeout is defined the command
    will be killed if the timeout is exceeded and an exception will be raised.

    Args:
        cmd (str): Command to execute
        timeout (int): Command timeout in seconds
    Returns:
        The command output which is STDOUT and STDERR merged.
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
        boutput, _ = phandle.communicate()
        output = boutput.decode(sys.stdout.encoding)
        retcode = phandle.poll()
    except _Signal:
        # Kill the running process
        phandle.kill()
        raise TimeoutError(cmd=cmd, timeout=timeout)
    except:
        logger.debug("Unhandled exception", exc_info=True)
        raise
    else:
        # Possible race condition where alarm isn't disabled in time
        signal.alarm(0)

    # Raise an exception if the command exited with non-zero exit status
    if retcode:
        raise RetcodeError(cmd, retcode, output=output)

    logger.debug(output)

    return output
