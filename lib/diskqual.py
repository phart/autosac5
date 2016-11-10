"""
diskqual.py

Disk qualification benchmark.

Copyright (C) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import sys
import os
import time
import subprocess
import logging
from lib.execute import RetcodeError


logger = logging.getLogger(__name__)


def dd(ifile, ofile, bs, duration):
    """
    dd wrapper

    Written for the dd syntax on NexentaStor.

    Args:
        ifile    (str): Input file
        ofile    (str): Output file
        bs       (str): Block size in KB
        duration (int): Timeout
    Returns:
        tput (int): Throughput in MB/s
    """
    ddcmd = "/usr/gnu/bin/dd"

    # Check that the dd command exists
    # On 3.x the command is in a seperate location
    if not os.path.isfile(ddcmd):
        raise RuntimeError("'%s' does not exist" % ddcmd)

    cmd = "%s if=%s of=%s bs=%sK" % (ddcmd, ifile, ofile, bs)

    logger.debug(cmd)

    try:
        # Execute the command and wait for the subprocess to terminate
        # STDERR is redirected to STDOUT
        ph = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
    except:
        raise

    # Sleep for duration
    time.sleep(duration)

    # Kill the running process if it is still running
    ph.send_signal(2)
    retcode = ph.wait()
    logger.debug("'%s' return code is %s", cmd, retcode)

    # Read the stdout/sterr buffers
    boutput, _ = ph.communicate()
    output = boutput.decode(sys.stdout.encoding)
    logger.debug(output)

    # Verify return code
    # A negative return code indicates the process received a signal
    if not (retcode == 0 or retcode == -2):
        raise RetcodeError(cmd, retcode, output=output.strip())

    # Split lines into fields
    records_in, records_out, summary = output.splitlines()

    # Parse for record counts
    records_in = int(records_in.split("+")[0])
    records_out = int(records_out.split("+")[0])

    # Parse for the byte total and time
    size = int(summary.split()[0])
    t = float(summary.split()[7])

    tput = size / t / 1024 ** 2

    return tput


def r_seq(disk, bs, duration):
    """
    Sequential disk read.

    Args:
        disk (str): Device ID
        bs (int): Block size in bytes
        duration (int): Test duration in seconds
    Returns:
        tput (float): Throughput in MB/s
    """
    logger.debug("r_seq test on %s", disk)

    try:
        tput = dd("/dev/rdsk/%ss0" % disk, "/dev/null", bs, duration)
    except:
        raise

    return tput
