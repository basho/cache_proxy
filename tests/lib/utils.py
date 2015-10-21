import os
import re
import sys
import time
import copy
import thread
import socket
import threading
import logging
import inspect
import argparse
import telnetlib
import redis
import random
import redis
import json
import glob
import commands

from collections import defaultdict
from argparse import RawTextHelpFormatter

from string import Template

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,  '../')

def getenv(key, default, conversion_func = None):
    value = os.environ.get(key, default)
    if conversion_func != None:
        try:
            value = conversion_func(value)
        except ValueError:
            value = default
    return value

default_retry_times = getenv('T_RETRY_TIMES', 5, int)
default_retry_delay = getenv('T_RETRY_DELAY', 0.1, float)

logfile = getenv('T_LOGFILE', 'log/t.log')
if logfile == '-':
    logging.basicConfig(level=logging.DEBUG,
        format="%(asctime)-15s [%(threadName)s] [%(levelname)s] %(message)s")
else:
    logging.basicConfig(filename=logfile, level=logging.DEBUG,
        format="%(asctime)-15s [%(threadName)s] [%(levelname)s] %(message)s")

logging.info("test running")

def strstr(s1, s2):
    return s1.find(s2) != -1

def lets_sleep(SLEEP_TIME = 0.1):
    time.sleep(SLEEP_TIME)

def TT(template, args): #todo: modify all
    return Template(template).substitute(args)

def TTCMD(template, args): #todo: modify all
    '''
    Template for cmd (we will replace all spaces)
    '''
    ret = TT(template, args)
    return re.sub(' +', ' ', ret)

def nothrow(ExceptionToCheck=Exception, logger=None):
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck, e:
                if logger:
                    logger.info(e)
                else:
                    print str(e)
        return f_retry  # true decorator
    return deco_retry

@nothrow(Exception)
def test_nothrow():
    raise Exception('exception: xx')

def json_encode(j):
    return json.dumps(j, indent=4, cls=MyEncoder)

def json_decode(j):
    return json.loads(j)

#commands dose not work on windows..
def system(cmd, log_fun=logging.info):
    if log_fun: log_fun(cmd)
    r = commands.getoutput(cmd)
    return r

def shorten(s, l=80):
    if len(s)<=l:
        return s
    return s[:l-3] + '...'

def assert_true(a):
    assert a, 'assert fail: expect true, got %s' % a

def assert_equal(a, b):
    assert a == b, 'assert fail: %s vs %s' % (shorten(str(a)), shorten(str(b)))

def assert_not_equal(a, b):
    assert a != b, 'assert fail: %s vs %s' % (shorten(str(a)), shorten(str(b)))

def assert_starts_with(a, b):
    a = str(a)
    b = str(b)
    c = b[0:len(a)]
    assert a == c, 'assert fail: %s vs %s for %s' % (shorten(a), shorten(c), shorten(b))

def assert_raises(exception_cls, callable, *args, **kwargs):
    try:
        callable(*args, **kwargs)
    except exception_cls as e:
        return e
    except Exception as e:
        assert False, 'assert_raises %s but raised: %s' % (exception_cls, e)
    assert False, 'assert_raises %s but nothing raise' % (exception_cls)

def assert_fail(err_response, callable, *args, **kwargs):
    try:
        callable(*args, **kwargs)
    except Exception as e:
        assert re.search(err_response, str(e)), \
               'assert "%s" but got "%s"' % (err_response, e)
        return

    assert False, 'assert_fail %s but nothing raise' % (err_response)

def assert_not_exception(a):
    assert not_exception_predicate(a), 'assert fail: expect not an exception, got %s' % a

def not_exception_predicate(obj):
    if inspect.isclass(obj) and issubclass(obj, Exception):
        return False
    return True

def object_notfound_ok_predicate(obj):
    if not not_exception_predicate(obj):
        return False
    if obj == None:
        return False
    return True

def object_found_predicate(obj):
    if not object_notfound_ok_predicate(obj):
        return False
    if obj == '':
        return False
    return True

def prime_connection_restart(server):
    # backoff and restart if the client isn't able to connect w/i a reasonable
    # time. This as well as retry_* functions were required to make the tests
    # stable enough for CI.
    sleep_s = getenv('T_PRIME_CONNECTION_DELAY', 0.5, float)
    sleep_s_pre = getenv('T_PRIME_CONNECTION_DELAY_PRE', sleep_s / 2.0, float)
    sleep_s_post = getenv('T_PRIME_CONNECTION_DELAY_POST', sleep_s / 2.0, float)
    server.stop()
    time.sleep(sleep_s_pre)
    server.start()
    time.sleep(sleep_s_post)

def prime_connection(r, server):
    sleep_s = default_retry_delay
    n = default_retry_times
    for i in range(0, n):
        first_response = retry_read(lambda : r.ping())
        if first_response.__class__.__name__ != 'ConnectionError':
            return
        prime_connection_restart(server)
        time.sleep(sleep_s)

def retry_read(read_func, n = None, sleep_s = None, \
        predicate = object_found_predicate):
    n = n or default_retry_times
    sleep_s = sleep_s or default_retry_delay

    obj = None
    while n > 0:
        n -= 1
        try:
            obj = read_func()
            if predicate(obj):
                return obj
        except BaseException as e:
            obj = e
        time.sleep(sleep_s)
    return obj

def retry_read_notfound_ok(read_func, n = None, sleep_s = None):
    return retry_read(read_func, n, sleep_s, object_notfound_ok_predicate)

def retry_write(write_func, n = None, sleep_s = None):
    return retry_read_notfound_ok(write_func, n, sleep_s)

def nutcracker_key(riak_key):
    return 'test:%s' % riak_key

def thread_id():
    return threading.current_thread().ident

def timestamp():
    return ('%.20f' % time.time()).replace('.','')

def distinct_key():
    return ('key%s%s' % (thread_id(), timestamp()))

def distinct_value():
    return ('val%s%s' % (thread_id(), timestamp()))

if __name__ == "__main__":
    test_nothrow()
