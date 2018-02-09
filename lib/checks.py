"""
checks.py

This module contains the system checks.

Copyright (c) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import socket
import logging
import requests
import lib.config as config
from time import sleep
from threading import Thread
from lib.nefclient import NEFClient
from lib.diskqual import r_seq
from queue import Queue, Empty
from lib.execute import execute, RetcodeError, TimeoutError


logger = logging.getLogger(__name__)


def check_ping(ip):
    """
    Ping a remote ip/hostname.

    Args:
        ip (str): IP address or hostname
    Returns:
        The check results.
    """
    # Network statistics
    result = {
        "host": ip,
        "success": True,
        "error": None
    }

    cmd = "ping -n -s %s 56 5" % ip
    try:
        # If it take more then 10s to ping 5 times there is something wrong
        output = execute(cmd, timeout=10)
    except RetcodeError as r:
        logger.error("%s is not alive", ip)
        result["success"] = False
        result["error"] = r.output
    except TimeoutError as t:
        logger.error(str(t))
        result["success"] = False
        result["error"] = str(t)
    else:
        logger.debug("''%s' is alive", ip)
        lline = output.splitlines()[-1]
        p_min, p_avg, p_max, p_stddev = lline.split("=")[1].strip().split("/")
        result["p_min"] = p_min
        result["p_avg"] = p_avg
        result["p_max"] = p_max
        result["p_stddev"] = p_stddev

    return result


def check_gateway_ping():
    """
    Check access and latency to the gateway server.

    Returns:
        The check results.
    """
    gateway = config.get_gateway()
    result = check_ping(gateway)

    return result


def check_dns_ping():
    """
    Check access and latency to each DNS server.

    Args:
        None
    Returns:
        The check results.
    """
    results = []
    nameservers = config.get_nameservers()

    # Ping each nameserver
    for n in nameservers:
        results.append(check_ping(n))

    return results


def check_domain_ping():
    """
    Check access and latency to the current domain server.

    Args:
        None
    Returns:
        The check results dict.
    """
    domain = config.get_domain()
    result = check_ping(domain)

    return result


def check_cmd(cmd, timeout=None):
    """
    Check command return code.

    Args:
        cmd (str): Bash command
        timeout (int): Command timeout in seconds
    Returns:
        The check results dict.
    """
    result = {
        "success": True,
        "error": None
    }

    logger.debug("check_cmd running \"%s\"", cmd)

    try:
        execute(cmd, timeout=timeout)
    except RetcodeError as r:
        logger.error("Failed with return code %s", r.retcode)
        logger.debug(r.output)
        result["success"] = False
        result["error"] = r.output
    except TimeoutError as t:
        logger.error(str(t))
        result["success"] = False
        result["error"] = str(t)

    return result


def check_dns_lookup(name):
    """
    Checks domain name resolution.

    Args:
        name (str): Domain name
    Returns:
        The check results.
    """
    result = {
        "success": True,
        "error": None
    }

    logger.debug("Attempting DNS resolution of %s", name)

    try:
        socket.gethostbyname(name)
    except socket.gaierror as e:
        logger.error("Failed to resolve %s", name)
        logger.debug(str(e), exc_info=True)
        result["success"] = False
        result["error"] = str(e)

    return result


def _rsf_move(cluster, service, fromnode, tonode):
    method = "rsf/clusters/%s/services/%s/move" % (cluster, service)
    payload = {
        "fromNode": fromnode,
        "toNode": tonode
    }
    result = {
        "name": service,
        "success": True,
        "error": None
    }

    logger.info("Move cluster service '%s' to '%s'", service, tonode)

    nef = NEFClient()
    try:
        jobid = nef.post(method, payload=payload)
    except requests.exceptions.HTTPError as e:
        logger.error("Failed to move cluster service '%s'", service)
        logger.debug(str(e), exc_info=True)
        result["success"] = False
        result["error"] = str(e)
    else:
        logger.info("Waiting for cluster service move to complete...")
        try:
            while not nef.jobstatus(jobid)[0]:
                sleep(10)
        except requests.exceptions.HTTPError as e:
            logger.error("Failed to move cluster service '%s'", service)
            logger.debug(str(e), exc_info=True)
            result["success"] = False
            result["error"] = str(e)

    return result


def check_rsf_move(local=True):
    """
    Check RSF service move.

    Args:
        local (bool): Move services local (True) or remote (False)
    Returns:
        The check results.
    """
    results = []
    hostname = config.get_hostname()
    cluster, partner, services = config.get_rsf()

    # Define the tonode and fromnode
    if local:
        tonode = hostname
        fromnode = partner
    else:
        tonode = partner
        fromnode = hostname

    # Failover all services
    for service in services:
        result = _rsf_move(cluster, service["serviceName"], fromnode, tonode)
        results.append(result)

    return results


def check_zpool_status():
    """
    Check zpool status and confirm all pools are ONLINE.

    Args:
        None:
    Returns:
        The check results.
    """
    results = []

    pools = config.get_pools()

    for p in pools:
        result = {
            "pool": p["poolName"],
            "success": True,
            "health": p["health"]
        }
        if p["health"] != "ONLINE":
            logger.error("The pool '%s' is not healthy", p["poolName"])
            result["success"] = False
        results.append(result)

    return results


def check_post(method, payload=None):
    """
    Check API POST request return code.

    Args:
        method (str): NEF API method
    Kwargs:
        payload (dict): Request payload
    Returns:
        Check results as a dictionary.
    """
    result = {
        "success": True,
        "error": None
    }

    try:
        nef = NEFClient()
        jobid = nef.post(method, payload=payload)
    except requests.exceptions.HTTPError as e:
        logger.error(str(e))
        result["status"] = False
        result["error"] = str(e)
    else:
        if jobid is not None:
            logger.info("Waiting for job to complete...")
            try:
                while not nef.jobstatus(jobid)[0]:
                    sleep(10)
            except requests.exceptions.HTTPError as e:
                logger.error("Job failed to complete")
                logger.debug(str(e), exc_info=True)
                result["success"] = False
                result["error"] = str(e)

    return result


def check_disk_perf(bs=32, duration=5, workers=8):
    """
    Verifies disk performance.

    Args:
        bs       (int): Blocksize in KB
        duration (int): Duration in seconds
        workers  (int): Number of threads
    Returns:
        The check results
    """
    disks = config.get_disks()
    resultsq = Queue()
    results = []

    def worker():
        # Iterate over queue
        while True:
            try:
                disk = diskq.get_nowait()
                logger.info("Verifying %s performance", disk)
            except Empty:
                break

            result = {
                "disk": disk,
                "success": True,
                "error": None
            }

            # Do something with disk
            try:
                tput = r_seq(disk, bs, duration)
            except RetcodeError as r:
                logger.error(str(r))
                logger.debug(r.output)
                result["success"] = False
                result["error"] = r.output
            # We don't want any unhandled exceptions while threading to we
            # will use this umbrella statement
            except Exception as e:
                logger.error("Failed %s with unhandled exception", disk)
                logger.error(str(e))
                logger.debug(str(e), exc_info=True)
                result["success"] = False
                result["error"] = str(e)
            else:
                logger.debug("%s performance is %s MB/s", disk, tput)
                result["tput"] = tput
            finally:
                resultsq.put(result)

    # Build queue
    diskq = Queue()
    [diskq.put(d["logicalDevice"]) for d in disks]

    # Start threads
    thrs = []
    for _ in range(workers):
        t = Thread(target=worker)
        t.start()
        thrs.append(t)

    # Join threads
    for t in thrs:
        t.join()

    # Build check dict from results
    while True:
        try:
            results.append(resultsq.get_nowait())
        except Empty:
            break

    return results


def check_metadata_blocks():
    """
    Verifies zfs_default_ibs is set to 14 (decimal) ; see NEX-15280

    Args:
        None
    Returns:
        The check results
    """
    result = {
        "success": True,
        "error": None
    }

    cmd = "echo 'zfs_default_ibs/D' | mdb -k"
    try:
        output = execute(cmd, timeout=10)
    except RetcodeError as r:
        logger.error("could not execute mdb command")
        result["success"] = False
        result["error"] = r.output
    except TimeoutError as t:
        logger.error(str(t))
        result["success"] = False
        result["error"] = str(t)
    else:
        # verify that the value is 14
        entry = output.split("\n")[1]
        value = int(entry.split(":")[1])
        if value != 14:
            logger.error("zfs_default_ibs is not 14! %s" % entry)
            result["success"] = False
            result["error"] = entry


    return result

