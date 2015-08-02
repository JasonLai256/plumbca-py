# -*- coding:utf-8 -*-

from functools import partial
import traceback

import msgpack
import zmq


packb = partial(msgpack.packb)
unpackb = partial(msgpack.unpackb, encoding='utf-8')


# Status codes
SUCCESS_STATUS = 1
FAILURE_STATUS = -1


def ms_to_sec(ms):
    """Returns an Integer approximated value"""
    return int(ms / 1000)


def sec_to_ms(sec):
    """Returns an Integer approximated value"""
    if isinstance(sec, float):
        return float(sec * 1000)
    return int(sec * 1000)


class MessageFormatError(Exception):
    pass


class Request(object):
    """Handler objects for frontend->backend objects messages"""
    def __new__(cls, cmd, *args):
        try:
            _args = {'args': args}
            args = packb(_args)
            content = [cmd, args]
        except KeyError:
            raise MessageFormatError("Invalid request format : %s" % str(kwargs))

        return content


def response_convert(raw_message):
    try:
        # print(raw_message)
        message = unpackb(raw_message)
        datas = message['datas']
        return datas
    except KeyError:
        print("Invalid response message : %s" % message)
        raise MessageFormatError("Invalid response message")


class Plumbca(object):

    def __init__(self, host='127.0.0.1', port='4273', transport='tcp', timeout=1):
        self.transport = transport
        self.endpoint = '%s:%s' % (host, port)
        self.host = "%s://%s" % (self.transport, self.endpoint)

        self.context = None
        self.socket = None

        self._timeout = sec_to_ms(timeout)
        self.setup_socket()

    def __del__(self):
        self.teardown_socket()

    def setup_socket(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.socket.connect(self.host)

    def teardown_socket(self):
        self.socket.close()
        self.context.term()

    @property
    def timeout(self):
        if not hasattr(self, '_timeout'):
            self._timeout = sec_to_ms(1)
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        value_in_ms = sec_to_ms(value)
        self._timeout = value_in_ms
        self.socket.setsockopt(zmq.RCVTIMEO, self._timeout)

    def send(self, msg, **kwargs):
        orig_timeout = ms_to_sec(self.timeout)  # Store updates is made from seconds
        timeout = kwargs.pop('timeout', 0)

        # If a specific timeout value was provided
        # store the old value, and update current timeout
        if timeout > 0:
            self.timeout = timeout

        self.socket.send_multipart(msg)

        try:
            raw_response = self.socket.recv_multipart()
        except zmq.ZMQError:
            # Timeout Occur
            # Restore original timeout and retun
            self.timeout = orig_timeout
            return FAILURE_STATUS

        # Restore original timeout
        self.timeout = orig_timeout

        return response_convert(raw_response[0])

    # def set_response_callback(self, command, callback):
    #     "Set a custom Response Callback"
    #     self.response_callbacks[command] = callback

    def execute_command(self, *args):
        "Execute a command and return a parsed response"
        command_name = args[0]
        try:
            rcontent = Request(command_name, *args[1:])
            response = self.send(rcontent)
            return response
        except Exception as err:
            error_track = traceback.format_exc()
            errmsg = '%s\n%s' % (err.message, error_track)
            errmsg = '<WORKER> Unknown situation occur: %s' % errmsg
            print(errmsg)

    def store(self, collection, timestamp, tagging, value):
        return self.execute_command('STORE', collection,
                                    timestamp, tagging, value)

    def query(self, collection, stime, etime, tagging):
        return self.execute_command('QUERY', collection, stime,
                                    etime, tagging)

    def fetch(self, collection, tagging='__all__', d=True, e=True):
        return self.execute_command('FETCH', collection, tagging, d, e)

    def wping(self):
        return self.execute_command('WPING')

    def ping(self):
        return self.execute_command('PING')

    def dump(self):
        return self.execute_command('DUMP')

    def ensure_collection(self, collection, class_type='IncreaseCollection',
                          expire=3600):
        return self.execute_command('ENSURE_COLLECTION', collection,
                                    class_type, expire)

    def get_collections(self):
        return self.execute_command('GET_COLLECTIONS')
