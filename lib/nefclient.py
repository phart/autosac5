"""
nefclient.py

A class for interacting with the NEF REST API.

Copyright (C) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""


import logging
import requests
import json
import __main__


logger = logging.getLogger(__name__)


class NEFClient(object):
    """
    NEF REST API client.

    WARNING this class does not currently validate the SSL certificate.

    Attributes:
        url (str): API url, i.e. https://<ip>
        port (int): API port
        username (str): Optional username, required if password provided
        password (str): Optional password, required if username provided
    """

    def __init__(self):
        self.port = 8443
        # We are grabbing username, password and url from __main__ to avoid
        # complex passing...this is a dirty hack
        self.url = ":".join([__main__.url, str(self.port)])
        self.username = __main__.username
        self.password = __main__.password
        self.key = None
        self.verify = False
        self.headers = {
            "Content-Type": "application/json"
        }

        # Disable security warnings
        #requests.packages.urllib3.disable_warnings()

        # Lets auth now so we can exit immediately if credentials are bad
        if self.username is not None and self.password is not None:
            self._login()
        elif self.username is not None:
            raise TypeError("A password is required when username is provided")
        elif self.password is not None:
            raise TypeError("A username is required when password is provided")

    def _login(self):
        """
        Sends a login request.
        """
        method = "auth/login"
        payload = {
            "username": self.username,
            "password": self.password
        }

        logger.debug("Logging in as user %s to %s", self.username, self.url)
        try:
            response = requests.post("/".join([self.url, method]),
                                     data=payload, verify=self.verify)
            response.raise_for_status()
            json = response.json()
        # Bookmark until I find out what error handling makes sense
        except:
            raise

        logger.debug(json)

        self.key = json["token"]
        self.headers["Authorization"] = "Bearer %s" % self.key

    def logout(self):
        """
        Sends logout request.
        """
        method = "auth/logout"
        logger.debug("Logging out as user %s on %s", self.username, self.url)
        self.post(method)

    def get(self, method, params=None):
        """
        Sends a GET request.

        Args:
            method (str): NEF API method
        Kwargs:
            params (dict): Request parameters
        Returns:
            The HTML response body as a dict.
        """
        logger.debug("GET %s", method)
        logger.debug(params)
        try:
            response = requests.get("/".join([self.url, method]),
                                    headers=self.headers, verify=self.verify,
                                    params=params)
            response.raise_for_status()
        # Bookmark until I find out what error handling makes sense
        except:
            raise

        # If there is no response body json() will fail
        try:
            body = response.json()
        except ValueError:
            body = None

        logger.debug(body)

        return body

    def post(self, method, payload=None):
        """
        Sends a POST request.

        Args:
            method (str): NEF API method
        Kwargs:
            payload (dict): Request payload
        Returns:
            The job ID if the request is ASYNC otherwise None.
        """
        logger.debug("POST %s", method)
        logger.debug(payload)
        try:
            response = requests.post("/".join([self.url, method]),
                                     headers=self.headers, verify=self.verify,
                                     data=json.dumps(payload))
            response.raise_for_status()
        # Bookmark until I find out what error handling makes sense
        except:
            raise

        # If there is no response body json() will fail
        try:
            body = response.json()
        except ValueError:
            body = None

        # If the status code is 202 the request is in progress
        if response.status_code == 202:
            jobid = body["links"][0]["href"].split("/")[-1]
        else:
            jobid = None

        logger.debug(body)

        return jobid

    def put(self, method, payload=None):
        """
        Sends a PUT request.

        Args:
            method (str): NEF API method
        Kwargs:
            payload (dict): Request payload
        Returns:
            The job ID if the request is ASYNC otherwise None.
        """
        logger.debug("PUT %s", method)
        logger.debug(payload)
        try:
            response = requests.put("/".join([self.url, method]),
                                    headers=self.headers, verify=self.verify,
                                    data=json.dumps(payload))
            response.raise_for_status()
        # Bookmark until I find out what error handling makes sense
        except:
            raise

        # If there is no response body json() will fail
        try:
            body = response.json()
        except ValueError:
            body = None

        # If the status code is 202 the request is in progress
        if response.status_code == 202:
            jobid = body["links"][0]["href"].split("/")[-1]
        else:
            jobid = None

        logger.debug(body)

        return jobid

    def delete(self, method, payload=None):
        """
        Sends a DELETE request.

        Args:
            method (str): NEF API method
        Kwargs:
            payload (dict): Request payload
        Returns:
            The job ID if the request is ASYNC otherwise None.
        """
        logger.debug("DELETE %s", method)
        logger.debug(payload)
        try:
            response = requests.delete("/".join([self.url, method]),
                                       headers=self.headers,
                                       verify=self.verify,
                                       data=json.dumps(payload))
            response.raise_for_status()
        # Bookmark until I find out what error handling makes sense
        except:
            raise

        # If there is no response body json() will fail
        try:
            body = response.json()
        except ValueError:
            body = None

        # If the status code is 202 the request is in progress
        if response.status_code == 202:
            jobid = body["links"][0]["href"].split("/")[-1]
        else:
            jobid = None

        logger.debug(body)

        return jobid

    def jobstatus(self, jobid):
        """
        Determine the ASYNC job status.

        Args:
            jobid (str): The job ID returned by the ASYN request
        Returns:
            The job ID progress and state (i.e. done or not).
        """
        method = "jobStatus"
        params = {
            "jobId": jobid
        }

        body = self.get(method, params=params)
        try:
            progress = body["data"][0]["progress"]
            done = body["data"][0]["done"]
        except IndexError:
            raise RuntimeError("The job ID no longer exists")

        return done, progress
