#!/usr/bin/env python
#coding: utf-8

from riak_common import *
import riak
import time
import redis

def test_backend():
    # ensure riak CRUD operations function correctly so we can test nutcracker
    # read-through w/o question as to whether the backend is working.
    key = distinct_key()
    value = distinct_value()
    (riak_client, riak_bucket, _, _) = getconn()
    riak_read_func = lambda : riak_bucket.get(key)
    riak_object = retry_read(riak_read_func)
    riak_write_func = lambda : riak_object.store()
    riak_delete_func = lambda : riak_object.delete()
    riak_object = retry_read_notfound_ok(riak_read_func)
    riak_object.data = value
    wrote = retry_write(riak_write_func)
    assert_not_exception(wrote)
    riak_object_readback = retry_read(riak_read_func)
    assert_equal(value, riak_object_readback.data)
    wrote = retry_write(riak_delete_func)
    assert_not_exception(wrote)
    riak_object_readback = retry_read_notfound_ok(riak_read_func)
    assert_equal(None, riak_object_readback.data)

def test_read_through_hit():
    multi_read_through(read_through_hit, 1)

def test_multi_read_through_hit():
    multi_read_through(read_through_hit, riak_multi_n)

def test_many_read_through_hit():
    multi_read_through(read_through_hit, riak_many_n)

def test_read_through_hit_large_key():
    key = 'K' * 255
    value = distinct_value()
    vs = {}
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    read_through_hit(vs, redis, riak_bucket, nutcracker, key, value)
    (value_written, value_read) = vs[key]
    assert_equal(value_written, value_read)

def test_read_through_hit_large_value():
    key = distinct_key()
    value = '0123456789ABCdef' * 500 #<< can be way larger, but 8Kb is sufficient
    vs = {}
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    read_through_hit(vs, redis, riak_bucket, nutcracker, key, value)
    (value_written, value_read) = vs[key]
    assert_equal(value_written, value_read)

def test_read_through_with_siblings():
    key = distinct_key()
    value = distinct_value()
    create_siblings(key)
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    vs = {}
    read_through_miss_get(vs, redis, riak_bucket, nutcracker, key, value)
    (value_written, value_read) = vs[key]
    assert_equal(value_written, value_read)

def test_read_through_miss_hit():
    multi_read_through(read_through_miss_hit, 1)

def test_multi_read_through_miss_hit():
    multi_read_through(read_through_miss_hit, riak_multi_n)

def test_many_read_through_miss_hit():
    multi_read_through(read_through_miss_hit, riak_many_n)

def test_read_through_miss_miss():
    multi_read_through(read_through_miss_miss, 1)

def test_multi_read_through_miss_miss():
    multi_read_through(read_through_miss_miss, riak_multi_n)

def test_many_read_through_miss_miss():
    multi_read_through(read_through_miss_miss, riak_many_n)

def multi_read_through(read_func, n):
    kvs = {}
    while len(kvs) < n:
        kvs[distinct_key()] = distinct_value()
    (riak_client, riak_bucket, nutcracker, redis) = getconn()

    vs = {}
    for key in kvs:
        value = kvs[key]
        t = threading.Thread(target = read_func, \
                args = (vs, redis, riak_bucket, nutcracker, key, value))
        t.daemon = True
        t.start()

    t1 = time.time()
    # max_wait here, but will timeout at the redis client
    max_wait = len(kvs) * 10.0
    while len(vs) < len(kvs):
        if time.time() - t1 > max_wait:
            break
        time.sleep(0.01)

    assert_equal(len(kvs), len(vs))
    for k in vs:
        (value_written, value_read) = vs[k]
        assert_equal(value_written, value_read)

def read_through_miss_miss(vs, redis, riak_bucket, nutcracker, key, _value):
    value = None
    nc_key = nutcracker_key(key)
    read_func = lambda : nutcracker.get(nc_key)
    expire_func = lambda : nutcracker.pexpire(nc_key, 0)
    riak_read_func = lambda: riak_bucket.get(key)
    riak_object = retry_read_notfound_ok(riak_read_func)
    riak_delete_func = lambda: riak_object.delete()
    wrote = retry_write(expire_func)
    assert_not_exception(wrote)
    cached_value = retry_read(read_func)
    wrote = retry_write(riak_delete_func)
    assert_not_exception(wrote)
    vs[key] = (value, cached_value)

def sibling_resolution_by_last_modified(riak_object):
    lm = lambda sibling: sibling.last_modified
    riak_object.siblings = [max(riak_object.siblings, key=lm), ]

def read_through_miss_get(vs, redis, riak_bucket, nutcracker, key, _value):
    riak_bucket.resolver = sibling_resolution_by_last_modified
    nc_key = nutcracker_key(key)
    read_func = lambda : nutcracker.get(nc_key)
    expire_func = lambda : nutcracker.pexpire(nc_key, 0)
    riak_read_func = lambda: riak_bucket.get(key)
    riak_object = retry_read_notfound_ok(riak_read_func)
    value = riak_object.data
    wrote = retry_write(expire_func)
    assert_not_exception(wrote)
    cached_value = retry_read(read_func)
    vs[key] = (value, cached_value)

def read_through_miss_hit(vs, _redis, riak_bucket, nutcracker, key, value):
    read_func = lambda : nutcracker.get(nutcracker_key(key))
    riak_read_func = lambda: riak_bucket.get(key)
    riak_object = retry_read_notfound_ok(riak_read_func)
    riak_write_func = lambda: riak_object.store()
    riak_object = retry_read_notfound_ok(riak_read_func)
    riak_object.data = value
    wrote = retry_write(riak_write_func)
    assert_not_exception(wrote)
    cached_value = retry_read(read_func)
    vs[key] = (value, cached_value)

def read_through_hit(vs, redis, riak_bucket, nutcracker, key, value):
    nc_key = nutcracker_key(key)
    read_func = lambda : nutcracker.get(nc_key)
    redis_write_func = lambda : redis.set(nc_key, value)
    riak_read_func = lambda: riak_bucket.get(key)
    riak_object = retry_read_notfound_ok(riak_read_func)
    riak_delete_func = lambda: riak_object.delete()
    wrote = retry_write(riak_delete_func)
    assert_not_exception(wrote)
    wrote = retry_write(riak_delete_func)
    assert_not_exception(wrote)
    wrote = retry_write(redis_write_func)
    assert_not_exception(wrote)
    cached_value = retry_read(read_func)
    vs[key] = (value, cached_value)
