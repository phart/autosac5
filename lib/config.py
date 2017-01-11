"""
config.py

This module contains functions for retrieving system configuration information.

Copyright (c) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import socket
import logging
from lib.nefclient import NEFClient


logger = logging.getLogger(__name__)


def get_hostname():
    """
    Return the system hostname.

    Args:
        None
    Returns:
        The system hostname.
    """
    try:
        hostname = socket.gethostname()
    except Exception as e:
        logger.debug(str(e), exc_info=True)
        raise RuntimeError("Failed to determine appliance hostname")

    logger.debug("The appliance hostname is %s", hostname)

    return hostname


def get_gateway():
    """
    Return the default network gateway.

    Args:
        None
    Returns:
        The default network gateway.
    """
    gateway = None
    method = "network/routes"
    params = {"destination": "default"}

    try:
        nef = NEFClient()
        body = nef.get(method, params=params)["data"]
    except Exception as exc:
        logger.debug(str(exc), exc_info=True)
        raise RuntimeError("Failed to determine network gateway")

    try:
        gateway = body[0]["gateway"]
    except IndexError:
        raise RuntimeError("No network gateway defined")

    logger.debug("The network gateway is %s", gateway)

    return gateway


def get_nameservers():
    """
    Return a list of configured DNS servers.

    Args:
        None
    Returns:
        A list of nameservers.
    """
    nameservers = []
    method = "network/nameservers"

    try:
        nef = NEFClient()
        body = nef.get(method)["data"]
    except Exception as e:
        logger.debug(str(e), exc_info=True)
        raise RuntimeError("Failed to determine appliance nameservers")
    for obj in body:
        ns = obj["nameserver"]
        logger.debug("Network nameserver %s", ns)
        nameservers.append(ns)

    if len(nameservers) == 0:
        raise RuntimeError("No network nameservers defined")

    return nameservers


def get_domain():
    """
    Return the domain configuration.

    Args:
        None
    Returns:
        domain (dict): Domain information
    """
    method = "services/smb"

    try:
        nef = NEFClient()
        body = nef.get(method)["sharingMode"]
    except Exception as e:
        logger.debug(str(e), exc_info=True)
        raise RuntimeError("Failed to determine domain configuration")

    if body["sharingMode"] != "domain":
        raise RuntimeError("The appliance is in %s mode" % body["sharingMode"])

    logger.debug("The appliance is connected to %s", body["realmName"])

    dc = body["domainController"]
    if not dc:
        raise RuntimeError("The appliance is in domain mode but isn't "
                           "connected to a DC")

    return dc


def get_rsf():
    """
    Return the RSF configuration.

    tank : -
    tank : nexenta1

    Args:
        None
    Returns:
        The cluster name, cluster partner, and cluster services.
    """
    method = "rsf/clusters"
    params = {"fields": "nodes,services"}

    try:
        nef = NEFClient()
        body = nef.get(method, params=params)["data"].pop()
    except IndexError:
        raise RuntimeError("The node is not part of a cluster")
    except Exception as e:
        logger.debug(str(e), exc_info=True)
        raise RuntimeError("Failed to determine cluster configuration")

    # Get and log cluster name
    cluster = body["clusterName"]
    logger.info("The node is a member of cluster '%s'", cluster)

    # Get RSF partner hostname
    hostname = get_hostname()
    for n in body["nodes"]:
        if n["machineName"] != hostname:
            partner = n["machineName"]
            break

    # Get cluster services
    services = body["services"]
    if not body["services"]:
        raise RuntimeError("There are no cluster services configured")

    return cluster, partner, services


def get_disks():
    """
    Return a list of attached disks device IDs.

    Args:
        None
    Returns:
        A list of attached device IDs.
    """
    method = "inventory/disks"

    try:
        nef = NEFClient()
        disks = nef.get(method)["data"]
    except Exception as e:
        logger.debug(str(e), exc_info=True)
        raise RuntimeError("Failed to determine disk configuration")

    if not disks:
        raise RuntimeError("No disks discovered")

    return disks


def get_pools():
    """
    Return a list of pools disks.

    Args:
        None
    Returns:
        A list of attached device IDs.
    """
    method = "storage/pools"

    try:
        nef = NEFClient()
        pools = nef.get(method)["data"]
    except Exception as e:
        logger.debug(str(e), exc_info=True)
        raise RuntimeError("Failed to determine pool configuration")

    if not pools:
        raise RuntimeError("No pools discovered")

    return pools
