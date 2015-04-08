#!/usr/bin/env python

"""
config.py

Parse system configuration.

Copyright (c) 2015  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import re
import os
import socket
import logging
from execute import execute, execute_nmc, Retcode, Timeout


_rsfcli = "/opt/HAC/RSF-1/bin/rsfcli -i0"
logger = logging.getLogger(__name__)


def get_hostname():
    """
    Return the system hostname.

    Inputs:
        None
    Outputs:
        hostname (str): Hostname
    """
    hostname = socket.gethostname()

    return hostname


def get_gateway_conf():
    """
    Return the default network gateway.

    NOTE we are parsing netstat -rn vs files because of inconsistencies between
    versions.

    Inputs:
        None
    Outputs:
        gateway (str): Default network gateway
    """
    gateway = None

    cmd = "route get default"
    try:
        output = execute(cmd)
    except Retcode, r:
        raise

    for l in output.splitlines():
        if l.strip().startswith("gateway"):
            gateway = l.split(":")[1].strip()
            break

    logger.debug("The network gateway is %s" % gateway)

    return gateway


def get_dns_conf():
    """
    Return the DNS servers.

    Inputs:
        None
    Outputs:
        dns (list): List of DNS servers
    """
    dns = []
    f = "/etc/resolv.conf"

    fh = open(f, "r")
    for line in fh:
        if line.startswith("nameserver"):
            dns.append(line.split()[1].strip())

    return dns


def get_domain_conf():
    """
    Return domain configuration.

    Inputs:
        None
    Outputs:
        domain (dict): Domain information
    """
    domain = {}
    cmd = "nltest /dsgetdcname"

    try:
        output = execute(cmd)
    except Retcode:
        return None

    # Parse all but the first line which contains header text
    for line in output.splitlines()[1:]:
        k, v = [x.strip() for x in line.split(":")]
        k = k.lower().replace(" ", "_")
        domain[k] = v

    return domain


def get_nmv_conf():
    """
    Return the NMV Apache configuration details.

    Inputs:
        None
    Ouputs:
        port (int): Web port
        https (bool): HTTPS enabled
    """
    port = None
    https = False
    apache_conf = "/etc/apache2/2.2/conf.d/nmv.conf"
    apache_conf_ssl = "/etc/apache2/2.2/conf.d/nmv-ssl.conf"

    if os.path.isfile(apache_conf):
        # Parse conf file for port
        fh = open(apache_conf, "r")
        for line in fh:
            if line.startswith("NameVirtualHost"):
                port = line.split(":")[1].strip()
                break
    elif os.path.isfile(apache_conf_ssl):
        https = True
        # Parse conf file for port
        fh = open(apache_conf_ssl, "r")
        for line in fh:
            if line.startswith("NameVirtualHost"):
                port = line.split(":")[1].strip()
                break
    else:
        raise RuntimeError("NMV Apache configuration does not exist")

    # If port is missing at this point the config is corrupt
    if port is None:
        raise RuntimeError("NMV Apache configuration is corrupt")

    return port, https


def get_rsf_conf():
    """
    Return the RSF configuration.

    tank : -
    tank : nexenta1

    Inputs:
        None
    Outputs:
        None
    """
    # Try to determine if the cluster is configured.
    # 4.x makes determination tricky because the service is installed
    # and running by default.

    # Make sure services are running
    if not _get_rsf_isrunning():
        raise RuntimeError("Cluster services aren't running")

    # Get RSF cluster name
    name = _get_rsf_name()

    # Check for default 4.x configuration
    if name == "Ready_For_Cluster_Configuration":
        raise RuntimeError("The cluster is unconfigured")

    # Get RSF partner hostname
    partner = _get_rsf_partner()
    # Get RSF services
    services = _get_rsf_services()

    return name, partner, services


def _get_rsf_name():
    """
    Return the RSF cluster name.

    Inputs:
        None
    Outputs:
        name (str): Cluster name
    """
    name = None

    try:
        output = execute("%s status" % _rsfcli)
    except:
        raise

    # Parse the line that start with 'Contacted' for the cluster name
    for l in output.splitlines():
        if l.startswith("Contacted"):
            name = l.split()[4].rstrip(",").strip("\"")

    return name


def _get_rsf_services():
    """
    Return RSF service list and status.

    Inputs:
        None
    Outputs:
        services (dict): name:host dictionary
    """
    services = {}

    try:
        output = execute("%s list" % _rsfcli)
    except:
        raise

    # Parse each line of otput and split on ':'
    for l in output.splitlines():
        svc, host = [x.strip() for x in output.split(":")]
        # Check if service is stopped
        if host == "-":
            host = None
        services[svc] = host

    return services


def _get_rsf_isrunning():
    """
    Return RSF service state.

    Inputs:
        None
    Ouputs:
        isrunning (bool): Service state
    """
    try:
        output = execute("%s isrunning" % _rsfcli)
    except Retcode:
        isrunning = False
    else:
        isrunning = True

    return isrunning


def _get_rsf_partner():
    """
    Return RSF partner host name.

    Inputs:
        None
    Outputs:
        partner (str): Partner hostname
    """
    partner = None

    try:
        output = execute("%s nodes" % _rsfcli)
    except:
        raise

    # This node contains '*' at the end of the line
    for l in output.splitlines():
        if "*" not in l:
            partner = l.strip()
            break

    return partner
