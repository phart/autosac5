#!/usr/bin/env python

"""
checks.py

System checks.

Copyright (c) 2015  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import socket
import urllib
import logging
from execute import execute, execute_nmc, Retcode, Timeout
from config import *


logger = logging.getLogger(__name__)


def check_ping(ip):
    """
    Ping a remote ip/hostname.

    Inputs:
        ip (str): IP address or hostname
    Outputs:
        alive (bool): Is IP reachable
    """
    # Network statistics
    check = {
        "status": None,
        "p_min": None,
        "p_avg": None,
        "p_max": None,
        "p_stddev": None
    }

    cmd = "ping -s %s 56 5" % ip
    try:
        output = execute(cmd)
    except Retcode:
        check["status"] = False
    else:
        check["status"] = True
        lline = output.splitlines()[-1]
        p_min, p_avg, p_max, p_stddev = lline.split("=")[1].strip().split("/")
        check["p_min"] = p_min
        check["p_avg"] = p_avg
        check["p_max"] = p_max
        check["p_stddev"] = p_stddev

    return check


def check_gateway_ping():
    """
    Check access and latency to the gateway server.

    Inputs:
        None
    Outputs:
        ping (list): Ping statistics
    """
    gateway = get_gateway_conf()
    check = check_ping(gateway)

    return check


def check_dns_ping():
    """
    Check access and latency to each DNS server.

    Inputs:
        None
    Outputs:
        ping (dict): Ping statistics
    """
    check = {}
    nameservers = get_dns_conf()

    # Ping each nameserver
    for n in nameservers:
        check[n] = check_ping(n)

    return check


def check_nmv_access():
    check = {
        "status": None,
        "output": None
    }

    try:
        port, https = get_nmv_conf()
    except RuntimeError, e:
        check["status"] = False
        check["output"] = str(e)
        return check

    if https:
        url = "https://localhost:%s/" % port
    else:
        url = "http://localhost:%s/" % port

    # Try to open the url
    try:
        rc = urllib.urlopen(url)
    except IOError, e:
        check["status"] = False
        check["output"] = str(e)
    else:
        if rc.code != 200:
            check["status"] = False
            check["output"] = "HTTP status %d" % rc.code
        else:
            check["status"] = True

    return check


def check_cmd(cmd):
    """
    Check command return code.

    Inputs:
        cmd (str): Bash command
    Outputs:
        None
    """
    check = {
        "status": None,
        "output": None
    }

    try:
        output = execute(cmd)
    except Retcode, r:
        check["status"] = False
        check["output"] = r.output
    except Timeout, t:
        check["status"] = False
        check["output"] = str(t)
    else:
        check["status"] = True
        check["output"] = output

    return check


def check_nmc_cmd(cmd):
    """
    Check NMC command return code.

    Inputs:
        cmd (str): Bash command
    Outputs:
        None
    """
    check = {
        "status": None,
        "output": None
    }

    try:
        output = execute_nmc(cmd)
    except Retcode, r:
        check["status"] = False
        check["output"] = r.output
    except Timeout, t:
        check["rc"] = False
        check["output"] = str(t)
    else:
        check["status"] = True
        check["output"] = output

    return check


def check_dns_lookup(name):
    """
    Checks domain name resolution.

    Inputs:
        name (str): Domain name
    Outputs:
        resolves (bool): Did resolution succeed
    """
    check = {
        "status": None,
        "output": None
    }

    try:
        socket.gethostbyname(name)
    except socker.gaierror, e:
        check["status"] = False
        check["output"] = str(e)
    else:
        check["status"] = True

    return check


def check_rsf_failover():
    check = {}
    hostname = get_hostname()
    name, partner, services = get_rsf_conf()

    # Failover any services running on this node
    for service, host in services.iteritems():
        if host != hostname:
            continue

        cmd = "setup group rsf-cluster %s shared-volume %s failover %s" \
              % (name, service, partner)
        check[service] = check_nmc_cmd(cmd)

    return check
