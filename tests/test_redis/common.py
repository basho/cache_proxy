#!/usr/bin/env python
#coding: utf-8

import os
import sys
import redis

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')
sys.path.append(os.path.join(WORKDIR,'lib/'))
sys.path.append(os.path.join(WORKDIR,'conf/'))

import conf

from server_modules import *
from server_modules_redis import *
from server_modules_nutcracker import *
from utils import *

CLUSTER_NAME = 'ntest'
nc_verbose = getenv('T_VERBOSE', 5, int)
mbuf = getenv('T_MBUF', 512, int)
large = getenv('T_LARGE', 1000, int)

all_redis = [
        RedisServer('127.0.0.1', 2100, '/tmp/r/redis-2100/', CLUSTER_NAME, 'redis-2100'),
        RedisServer('127.0.0.1', 2101, '/tmp/r/redis-2101/', CLUSTER_NAME, 'redis-2101'),
        ]

nc = NutCracker('127.0.0.1', 4100, '/tmp/r/nutcracker-4100', CLUSTER_NAME,
                all_redis, mbuf=mbuf, verbose=nc_verbose)

def setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    for r in all_redis + [nc]:
        r.clean()
        r.deploy()
        r.stop()
        r.start()

def teardown():
    for r in all_redis + [nc]:
        if(getenv('T_CHECK_ALIVE_ON_TEARDOWN', False)):
            assert(r._alive())
        r.stop()

default_kv = {'kkk-%s' % i : 'vvv-%s' % i for i in range(10)}

def getconn():
    for r in all_redis:
        c = redis.Redis(r.host(), r.port())
        c.flushdb()

    r = redis.Redis(nc.host(), nc.port())
    prime_connection(r, nc)
    return r

