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
from execute import Retcode


logger = logging.getLogger(__name__)


def dd(ifile, ofile, bs, duration):
    """
    dd wrapper

    Written for the dd syntax on NexentaStor.

    Inputs:
        ifile    (str): Input file
        ofile    (str): Output file
        bs       (str): Block size in KB
        duration (int): Timeout
    Outputs:
        tput (int): Throughput in MB/s
    """
    dd = "/usr/gnu/bin/dd"

    # Check that the dd command exists
    # On 3.x the command is in a seperate location
    if not os.path.isfile(dd):
        raise RuntimeError("%s does not exist" % dd)

    cmd = "%s if=%s of=%s bs=%sK" % (dd, ifile, ofile, bs)

    logger.debug("Executing \"%s\"" % cmd)

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
    logger.debug("\"%s\" return code is %s" % (cmd, retcode))

    # Read the stdout/sterr buffers
    output, _ = ph.communicate()
    logger.debug(output)

    # Verify return code
    # A negative return code indicates the process received a signal
    if not (retcode == 0 or retcode == -2):
        raise Retcode(cmd, retcode, output=output.strip())

    # Split lines into fields
    records_in, records_out, summary = output.splitlines()

    # Parse for record counts
    records_in = int(records_in.split("+")[0])
    records_out = int(records_out.split("+")[0])

    # Parse for the byte total and time
    size = int(summary.split()[0])
    t = float(summary.split()[5])

    tput = size / t / 1024 ** 2

    return tput


def r_seq(disk, bs, duration):
    """
    Sequential disk read.

    Inputs:
        disk     (str): Device ID
        bs       (int): Block size in bytes
        duration (int): Test duration in seconds
    Outputs:
        tput (float): Throughput in B/s
        iops (float): IOPS
    """
    logger.debug("r_seq test on %s" % disk)

    try:
        tput = dd("/dev/rdsk/%ss0" % disk, "/dev/null", bs, duration)
    except:
        raise

    return tput
