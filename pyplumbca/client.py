# -*- coding:utf-8 -*-

from functools import partial
import traceback
import logging

import msgpack

from .handler import Handler


packb = partial(msgpack.packb)
unpackb = msgpack.unpackb
# unpackb = partial(msgpack.unpackb, encoding='utf-8')


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
            cmd = cmd.encode('utf8')
            args = packb({'args': args})
            content = b' '.join([cmd, args])
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
        logging.error("Invalid response message : %s", message)
        raise MessageFormatError("Invalid response message")


class Plumbca(object):

    def __init__(self, host='127.0.0.1', port='4273', proto='tcp', timeout=10):
        self.handler = Handler(host=host, port=port,
                               proto=proto, timeout=timeout)

    def __del__(self):
        self.handler._close()

    def send(self, msg, **kwargs):
        try:
            self.handler.send(msg)
            raw_response = self.handler.recv()
            self.handler._close()
        except Exception:
            logging.error(msg, kwargs)
            raise

        return response_convert(raw_response)

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
            logging.error(errmsg)
            return FAILURE_STATUS

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
