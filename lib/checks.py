#!/usr/bin/env python

"""
checks.py

System checks.

Copyright (c) 2015  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import socket
import urllib2
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
        check (dict): Check results
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
        logger.error("%s is not alive" % ip)
        check["status"] = False
    else:
        logger.debug("%s is alive" % ip)
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
        check (dict): Check results
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
        check (dict): Check results
    """
    check = {}
    nameservers = get_dns_conf()

    # Ping each nameserver
    for n in nameservers:
        check[n] = check_ping(n)

    return check


def check_nmv_access():
    """
    Check access to the web interface.

    Inputs:
        None
    Outputs:
        check (dict): Check results
    """
    check = {
        "status": None,
        "output": None
    }
    vers = get_major_vers()
    if vers == 3:
        port, https = get_nmv_conf_3()
    else:
        port, https = get_nmv_conf()

    if https:
        url = "https://localhost:%s/" % port
    else:
        url = "http://localhost:%s/" % port

    logger.debug("check_nmv_access verifying url %s" % url)

    # Try to open the url
    try:
        rc = urllib2.urlopen(url)
    except IOError, e:
        logger.error("Failed to reach NMV")
        logger.debug(str(e))
        check["status"] = False
        check["output"] = str(e)
    else:
        if rc.code != 200:
            check["status"] = False
            check["output"] = "Failed with HTTP status %d" % rc.code
            logger.error(" HTTP status %s" % rc.code)
        else:
            logger.debug("Succeeded")
            check["status"] = True

    return check


def check_domain_ping():
    """
    Check access and latency to the current domain server.

    Inputs:
        None
    Outputs:
        check (dict): Check results
    """
    vers = get_major_vers()
    if vers == 3:
        domain = get_domain_conf_3()["dc_addr"]
    else:
        domain = get_domain_conf()["dc_addr"]

    check = check_ping(domain)

    return check


def check_cmd(cmd):
    """
    Check command return code.

    Inputs:
        cmd (str): Bash command
    Outputs:
        check (dict): Check results
    """
    check = {
        "status": None,
        "output": None
    }

    logger.debug("check_cmd running \"%s\"" % cmd)

    try:
        output = execute(cmd)
    except Retcode, r:
        logger.error("Failed with return code %s" % r.retcode)
        logger.debug(r.output)
        check["status"] = False
        check["output"] = r.output
    except Timeout, t:
        logger.error("Timed out after %ss" % t.timeout)
        check["status"] = False
        check["output"] = str(t)
    else:
        logger.debug("Succeeded")
        check["status"] = True
        check["output"] = output

    return check


def check_nmc_cmd(cmd):
    """
    Check NMC command return code.

    Inputs:
        cmd (str): Bash command
    Outputs:
        check (dict): Check results
    """
    check = {
        "status": None,
        "output": None
    }

    logger.debug("Running NMC command \"%s\"" % cmd)

    try:
        output = execute_nmc(cmd)
    except Retcode, r:
        logger.error("Failed with return code %s" % r.retcode)
        logger.debug(r.output)
        check["status"] = False
        check["output"] = r.output
    except Timeout, t:
        logger.error("Timed out after %ss" % t.timeout)
        check["rc"] = False
        check["output"] = str(t)
    else:
        logger.debug("Succeeded")
        check["status"] = True
        check["output"] = output

    return check


def check_dns_lookup(name):
    """
    Checks domain name resolution.

    Inputs:
        name (str): Domain name
    Outputs:
        check (dict): Check results
    """
    check = {
        "status": None,
        "output": None
    }

    logger.debug("Attempting DNS resolution of %s" % name)

    try:
        socket.gethostbyname(name)
    except socker.gaierror, e:
        logger.error("Failed to resolve %s" % name)
        check["status"] = False
        check["output"] = str(e)
    else:
        logger.debug("Succeeded")
        check["status"] = True

    return check


def check_rsf_failover():
    """
    Check RSF service failover.

    NOTE the check will only failover services that are active on the host the
    check is running on.

    Inputs:
        None
    Outputs:
        check (dict): Check results
    """
    check = {}
    hostname = get_hostname()
    name, partner, services = get_rsf_conf()

    # Failover any services running on this node
    for service, host in services.iteritems():
        if host != hostname:
            logger.debug("Fail over for %s skipped" % service)
            continue

        logger.info("Failover %s to %s" % (service, hostname))

        cmd = "setup group rsf-cluster %s shared-volume %s failover %s" \
              % (name, service, partner)
        check[service] = check_nmc_cmd(cmd)

    return check
