# -*- coding:utf-8 -*-

"""
Inspired from the Diamond project [https://github.com/BrightcoveOS/Diamond].

Base on the MIT license.
"""

import struct
import socket
import time
import logging


class Handler():
    """
    Implements the abstract Handler class, sending data to plumbca
    """

    def __init__(self, **kwargs):
        """
        Create a new instance of the Handler class

        Configure include:
            - host: Hostname
            - port: Port
            - proto: udp, udp4, udp6, tcp, tcp4, or tcp6
            - timeout:
            - keepalive: Enable keepalives for tcp streams
            - keepaliveinterval: How frequently to send keepalives
            - flow_info: IPv6 Flow Info
            - scope_id: IPv6 Scope ID
        """
        # Initialize Log
        self.log = logging

        # Initialize Blank Configs
        self.config = {}

        # Load default configure
        self.config.update(self.get_default_config())

        # Load in user configure
        self.config.update(kwargs)

        # Initialize Data
        self.socket = None

        # Initialize Options
        self.proto = self.config['proto'].lower().strip()
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.timeout = int(self.config['timeout'])
        self.keepalive = bool(self.config['keepalive'])
        self.keepaliveinterval = int(self.config['keepaliveinterval'])
        self.flow_info = self.config['flow_info']
        self.scope_id = self.config['scope_id']

        # Connect
        self._connect()

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = {
            'host': 'localhost',
            'port': 4273,
            'proto': 'tcp',
            'timeout': 15,
            'keepalive': 0,
            'keepaliveinterval': 10,
            'flow_info': 0,
            'scope_id': 0,
        }

        return config

    def __del__(self):
        """
        Destroy instance of the Handler class
        """
        self._close()

    def flush(self):
        """Flush data in queue"""
        self._send()

    def _send_data(self, data):
        """
        Try to send all data in buffer.
        """
        try:
            self.socket.sendall(data+'\n')
        except:
            self._close()
            self.log.error("Handler: Socket error, "
                           "trying reconnect.")
            self._connect()
            try:
                self.socket.sendall(data+'\n')
            except:
                return

    def send(self, data):
        """
        Send data to graphite. Data that can not be sent will be queued.
        """
        # Check to see if we have a valid socket. If not, try to connect.
        try:
            if self.socket is None:
                self.log.debug("Handler: Socket is not connected. "
                               "Reconnecting.")
                self._connect()
            if self.socket is None:
                self.log.debug("Handler: Reconnect failed.")
            else:
                # Send data to socket
                self._send_data(data)
                # self._close()
        except Exception:
            self._close()
            self.log.error("Handler: Error sending data.")
            raise

    def recv(self):
        # total data partwise in an array
        total_data = []
        data = ''

        begin = time.time()
        while 1:
            # if you got some data, then break after timeout
            if total_data and time.time() - begin > self.timeout:
                break

            # if you got no data at all, wait a little longer, twice the timeout
            elif time.time() - begin > self.timeout * 2:
                break

            # recv something
            try:
                data = self.socket.recv(8192)
                if data:
                    total_data.append(data)
                    # change the beginning time for measurement
                    begin = time.time()
                else:
                    break
            except:
                pass

        # join all parts to make final string
        return ''.join(total_data)

    def _connect(self):
        """
        Connect to the graphite server
        """
        if (self.proto == 'udp'):
            stream = socket.SOCK_DGRAM
        else:
            stream = socket.SOCK_STREAM

        if (self.proto[-1] == '4'):
            family = socket.AF_INET
            connection_struct = (self.host, self.port)
        elif (self.proto[-1] == '6'):
            family = socket.AF_INET6
            connection_struct = (self.host, self.port,
                                 self.flow_info, self.scope_id)
        else:
            connection_struct = (self.host, self.port)
            addrinfo = socket.getaddrinfo(self.host, self.port, 0, stream)
            if (len(addrinfo) > 0):
                family = addrinfo[0][0]
                if (family == socket.AF_INET6):
                    connection_struct = (self.host, self.port,
                                         self.flow_info, self.scope_id)
            else:
                family = socket.AF_INET

        # Create socket
        self.socket = socket.socket(family, stream)
        if self.socket is None:
            # Log Error
            self.log.error("Handler: Unable to create socket.")
            # Close Socket
            self._close()
            return
        # Enable keepalives?
        if self.proto != 'udp' and self.keepalive:
            self.log.error("Handler: Setting socket keepalives...")
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,
                                   self.keepaliveinterval)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
                                   self.keepaliveinterval)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        # Set socket timeout
        self.socket.settimeout(self.timeout)
        # Connect to graphite server
        try:
            self.socket.connect(connection_struct)
            # Log
            self.log.debug("Handler: Established connection to "
                           "graphite server %s:%d.",
                           self.host, self.port)
        except Exception, ex:
            # Log Error
            self.log.error("Handler: Failed to connect to "
                           "%s:%i. %s.", self.host, self.port, ex)
            # Close Socket
            self._close()
            return

    def _close(self):
        """
        Close the socket
        """
        if self.socket is not None:
            self.socket.close()
        self.socket = None
