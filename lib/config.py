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


def get_major_vers():
    """
    Return major version number, i.e. for 4.0.3 we would return 4.

    Inputs:
        None
    Outputs:
        vers (int): Major version
    """
    vers = None

    cmd = "uname -a"
    try:
        output = execute(cmd)
    except:
        logger.error("Failed to determine appliance version", exc_info=1)
        raise RuntimeError("Failed to determine appliance version")

    vers = output.split("NexentaOS_")[1]
    if vers.startswith("134f"):
        vers = 3
    else:
        vers = vers[0]

    logger.debug("The appliance version is %s" % vers)

    return vers


def get_hostname():
    """
    Return the system hostname.

    Inputs:
        None
    Outputs:
        hostname (str): Hostname
    """
    try:
        hostname = socket.gethostname()
    except:
        logger.error("Failed to determine appliance hostname", exc_info=1)
        raise RuntimeError("Failed to determine appliance hostname")

    logger.debug("System hostname is %s" % hostname)

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

    cmd = "route -n get default"
    try:
        output = execute(cmd)
    except:
        logger.error("Failed to determine network gateway", exc_info=1)
        raise RuntimeError("Failed to determine network gateway")

    for l in output.splitlines():
        if l.strip().startswith("gateway"):
            gateway = l.split(":")[1].strip()
            break

    if gateway is None:
        logger.error("No network gateway defined")
        logger.error(output)
        raise RuntimeError("No network gateway defined")

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

    try:
        fh = open(f, "r")
    except:
        logger.error("Unable to open resolv.conf", exc_info=1)
        raise RuntimeError("Failed to determine appliance nameservers")
    for line in fh:
        if line.startswith("nameserver"):
            d = line.split()[1].strip()
            logger.debug("Network nameserver %s" % d)
            dns.append(d)

    if len(dns) == 0:
        logger.error("No network nameservers defined")
        logger.error(fh.read())
        raise RuntimeError("No network nameservers defined")

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
    except:
        logger.error("Failed to determine appliance domain configuration",
                     exc_info=1)
        raise RuntimeError("Failed to determine appliance domain " \
                           "configuration")

    # Parse all but the first line which contains header text
    for line in output.splitlines()[1:]:
        k, v = [x.strip() for x in line.split(":")]
        k = k.lower().replace(" ", "_")
        logger.debug("Domain %s: %s" % (k, v))
        domain[k] = v

    # Log and raise an exception if the dict is empty
    # In theory if we make it this far domain should not be empty
    if not domain:
        logger.error("No domain configuration defined")
        logger.error(output)
        raise RuntimeError("No domain configuration defined")

    return domain


def get_domain_conf_3():
    """
    Legacy function for version 3.x. Returns domain configuration.

    Inputs:
        None
    Outputs:
        domain (str): DC IP
    """
    domain = {}

    cmd = "smbadm list"
    try:
        output = execute(cmd)
    except:
        logger.error("Failed to determine appliance domain configuration",
                     exc_info=1)
        raise RuntimeError("Failed to determine appliance domain " \
                           "configuration")

    # Parse out domain name, DC hostname and IP
    name, dc = output.splitlines()[1:3]
    domain["domain_name"] = name.split()[1].lstrip("[").rstrip("]")
    dc_name, dc_addr = dc.split()
    domain["dc_name"] = dc_name.lstrip("[+").rstrip("]")
    domain["dc_addr"] = dc_addr.lstrip("[").rstrip("]")

    # Log and raise an exception if the dict is empty
    # In theory if we make it this far domain should not be empty
    if not domain:
        logger.error("No domain configuration defined")
        logger.error(output)
        raise RuntimeError("No domain configuration defined")

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

    # Check for existince of Apache HTTP conf
    if os.path.isfile(apache_conf):
        logger.debug("NMV is not configured with https")
        # Parse conf file for port
        port = _get_nmv_port(apache_conf)
    # Check for the existince of Apache HTTPS conf
    elif os.path.isfile(apache_conf_ssl):
        logger.debug("NMV is configured with https")
        https = True
        port = _get_nmv_port(apache_conf_ssl)
    else:
        raise RuntimeError("Apache configuration file does not exist")

    return port, https


def get_nmv_conf_3():
    """
    Legacy function for version 3.x. Return the NMV Apache configuration
    details.

    Inputs:
        None
    Ouputs:
        port (int): Web port
        https (bool): HTTPS enabled
    """
    port = None
    https = False
    apache_conf = "/etc/apache2/sites-enabled/nmv"
    apache_conf_ssl = "/etc/apache2/sites-enabled/nmv-ssl"

    # Check for existince of Apache HTTP conf
    if os.path.isfile(apache_conf):
        logger.debug("NMV is not configured with https")
        # Parse conf file for port
        port = _get_nmv_port(apache_conf)
    # Check for the existince of Apache HTTPS conf
    elif os.path.isfile(apache_conf_ssl):
        logger.debug("NMV is configured with https")
        https = True
        port = _get_nmv_port(apache_conf_ssl)
    else:
        raise RuntimeError("Apache configuration file does not exist")

    return port, https


def _get_nmv_port(f):
    """
    Parse the apache configuration file for the port number.

    Inputs:
        f (str): Path to configuration file
    Outputs:
        port (int): Port number
    """
    port = None

    try:
        fh = open(f, "r")
    except:
        logger.error("Failed to open the Apache configuration file",
                     exc_info=1)
        raise RuntimeError("Failed to determine appliance NMV port")
    for line in fh:
        if line.startswith("NameVirtualHost"):
            port = line.split(":")[1].strip()
            logger.debug("NMV is running on port %s" % port)
            break

    # If port is missing at this point the config is corrupt
    if port is None:
        logger.error("Apache configuration is corrupt")
        logger.error(fh.read())
        raise RuntimeError("Apache configuration is corrupt")

    return port


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
        raise RuntimeError("RSF cluster services aren't running")

    # Get RSF cluster name
    name = _get_rsf_name()

    # Check for default 4.x configuration
    if name == "Ready_For_Cluster_Configuration":
        raise RuntimeError("RSF cluster services are unconfigured")

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
        logger.error("Failed to determine RSF cluster name", exc_info=1)
        raise RuntimeError("Failed to determine RSF cluster name")

    # Parse the line that start with 'Contacted' for the cluster name
    for l in output.splitlines():
        if l.startswith("Contacted"):
            name = l.split()[4].rstrip(",").strip("\"")

    if name is None:
        logger.error("No RSF cluster name defined")
        logger.error(output)
        raise RuntimeError("No RSF cluster name defined")

    logger.debug("RSF cluster name is %s" % name)

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
        logger.error("Failed to determine RSF services", exc_info=1)
        raise RunTimeError("Failed to determine RSF services")

    # Parse each line of otput and split on ':'
    for l in output.splitlines():
        svc, host = [x.strip() for x in output.split(":")]
        # Check if service is stopped
        if host == "-":
            host = None
        logger.debug("RSF service %s is running on %s" % (svc, host))
        services[svc] = host

    if not services:
        logger.error("No RSF services defined")
        logger.error(output)
        raise RuntimeError("No RSF services defined")

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
        logger.debug("RSF service is disabled")
        isrunning = False
    except:
        logger.error("Failed to determine RSF status", exc_info=1)
        raise RuntimeError("Failed to determine RSF status")
    else:
        logger.debug("RSF service is enabled")
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
        logger.error("Failed to determine appliance RSF partner", exc_info=1)
        raise RuntimeError("Failed to determine appliance RSF partner")

    # This node contains '*' at the end of the line
    for l in output.splitlines():
        if "*" not in l:
            partner = l.strip()
            break

    if partner is None:
        raise RuntimeError("Failed to determine appliance RSF partner")

    logger.debug("RSF partner is %s" % partner)

    return partner
