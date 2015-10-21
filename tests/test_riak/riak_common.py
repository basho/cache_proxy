#!/usr/bin/env python
#coding: utf-8

import os
import sys
import redis
import riak

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')
sys.path.append(os.path.join(WORKDIR,'lib/'))
sys.path.append(os.path.join(WORKDIR,'conf/'))

import conf

from server_modules import *
from server_modules_redis import *
from server_modules_riak import *
from server_modules_nutcracker import *
from utils import *

# NOTE: such naive thread-per-request implementation as opposed to thread pool
# doesn't scale far enough for load testing, but here we are testing concurrent
# requests intentionally, to help establish max concurrent supported.
riak_multi_n = getenv('T_RIAK_MULTI', 2, int)
riak_many_n = getenv('T_RIAK_MANY', 50, int)

CLUSTER_NAME = 'ntest2'
nc_verbose = getenv('T_VERBOSE', 5, int)
mbuf = getenv('T_MBUF', 512, int)
large = getenv('T_LARGE', 1000, int)
all_redis = [
        RedisServer('127.0.0.1', 2110, '/tmp/r/redis-2110/', CLUSTER_NAME, 'redis-2110'),
        ]

riak_cluster = RiakCluster([('devB', 5200)])

nc = NutCracker('127.0.0.1', 4210, '/tmp/r/nutcracker-4210', CLUSTER_NAME,
                all_redis, mbuf=mbuf, verbose=nc_verbose, riak_cluster=riak_cluster)

def setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    riak_cluster.deploy()
    riak_cluster.start()
    for r in all_redis + [riak_cluster, nc]:
        r.clean()
        r.deploy()
        r.stop()
        r.start()

def teardown():
    for r in [nc, riak_cluster] + all_redis:
        if not r._alive():
            print('%s was not alive at teardown' % r)
        r.stop()

def getconn():
    for r in all_redis:
        c = redis.Redis(r.host(), r.port())
        c.flushdb()

    riak_client = riak.RiakClient(pb_port = riak_cluster.port(), protocol = 'pbc')
    riak_bucket = riak_client.bucket('test')

    nutcracker = redis.Redis(nc.host(), nc.port())
    r = redis.Redis(all_redis[0].host(), all_redis[0].port())

    return (riak_client, riak_bucket, nutcracker, r)

def ensure_siblings_bucket_properties():
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    bucket_properties = riak_bucket.get_properties()
    if not bucket_properties['allow_mult'] or bucket_properties['last_write_wins']:
        riak_bucket.set_property('allow_mult', True)
        riak_bucket.set_property('last_write_wins', False)
        bucket_properties_readback = riak_bucket.get_properties()
        assert_equal(True, bucket_properties_readback['allow_mult'])
        assert_equal(False, bucket_properties_readback['last_write_wins'])

def create_siblings(key):
    ensure_siblings_bucket_properties()
    value = distinct_value()
    value2 = distinct_value()
    assert_not_equal(value, value2)
    (_riak_client, riak_bucket, _nutcracker, _redis) = getconn()
    riak_object = riak_bucket.get(key)
    riak_object2 = riak_bucket.get(key)
    riak_object.data = value
    riak_object.store()
    riak_object2.data = value2
    riak_object2.store()
    riak_object_readback = riak_bucket.get(key)
    assert_not_equal('[]', siblings_str(riak_object_readback))

def siblings_str(riak_object):
    return str([ str(content.data) for content in riak_object.siblings ])

