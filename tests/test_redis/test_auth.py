#!/usr/bin/env python
#coding: utf-8

from common import *

all_redis = [
    RedisServer('127.0.0.1', 2100, '/tmp/r/redis-2100/',
                CLUSTER_NAME, 'redis-2100', auth = 'hellopasswd'),
    RedisServer('127.0.0.1', 2101, '/tmp/r/redis-2101/',
                CLUSTER_NAME, 'redis-2101', auth = 'hellopasswd'),
]

nc = NutCracker('127.0.0.1', 4100, '/tmp/r/nutcracker-4100', CLUSTER_NAME,
                all_redis, mbuf=mbuf, verbose=nc_verbose,
                redis_auth = 'hellopasswd')

nc_badpass = NutCracker('127.0.0.1', 4101, '/tmp/r/nutcracker-4101', CLUSTER_NAME,
                        all_redis, mbuf=mbuf, verbose=nc_verbose,
                        redis_auth = 'badpasswd')
nc_nopass = NutCracker('127.0.0.1', 4102, '/tmp/r/nutcracker-4102', CLUSTER_NAME,
                       all_redis, mbuf=mbuf, verbose=nc_verbose)

def setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    for r in all_redis + [nc, nc_badpass, nc_nopass]:
        r.clean()
        r.deploy()
        r.stop()
        r.start()

def teardown():
    for r in all_redis + [nc, nc_badpass, nc_nopass]:
        if(getenv('T_CHECK_ALIVE_ON_TEARDOWN', False)):
            assert(r._alive())
        r.stop()

default_kv = {'kkk-%s' % i : 'vvv-%s' % i for i in range(10)}

'''

cases:


redis       proxy       case
1           1           test_auth_basic
1           bad         test_badpass_on_proxy
1           0           test_nopass_on_proxy
0           0           already tested on other case
0           1

'''

def auth_basic(r):
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.ping())))
    assert_starts_with('NOAUTH', str(retry_write(lambda : r.set('k', 'v'))))
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.get('k'))))

    # bad passwd
    assert_equal('invalid password', str(retry_write(lambda : r.execute_command('AUTH', 'badpasswd'))))

    # everything is ok after auth
    retry_write(lambda : r.execute_command('AUTH', 'hellopasswd'))
    retry_write(lambda : r.set('k', 'v'))
    assert_equal(True, retry_read(lambda : r.ping()))
    assert_equal('v', retry_read(lambda : r.get('k')))

    # auth fail here, should we return ok or not => we will mark the conn state as not authed
    assert_equal('invalid password', str(retry_write(lambda : r.execute_command('AUTH', 'badpasswd'))))
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.ping())))
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.get('k'))))

def test_auth_basic_redis():
    r = redis.Redis(all_redis[0].host(), all_redis[0].port())
    prime_connection(r, all_redis[0])
    auth_basic(r)

def test_auth_basic_proxy():
    r = redis.Redis(nc.host(), nc.port())
    prime_connection(r, nc)
    auth_basic(r)

def test_nopass_on_proxy():
    r = redis.Redis(nc_nopass.host(), nc_nopass.port())
    prime_connection(r, nc_nopass)
    # if you config pass on redis but not on twemproxy,
    # twemproxy will reply ok for ping, but once you do get/set, you will get errmsg from redis
    assert_equal(True, retry_read(lambda : r.ping()))
    assert_starts_with('NOAUTH', str(retry_write(lambda : r.set('k', 'v'))))
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.get('k'))))

    # proxy has no pass, when we try to auth
    assert_starts_with('Client sent AUTH, but no password is set', str(retry_write(lambda : r.execute_command('AUTH', 'anypasswd'))))

def test_badpass_on_proxy():
    r = redis.Redis(nc_badpass.host(), nc_badpass.port())
    prime_connection(r, nc_badpass)
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.ping())))
    assert_starts_with('NOAUTH', str(retry_write(lambda : r.set('k', 'v'))))
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.get('k'))))

    # we can auth with bad pass (twemproxy will say ok for this)
    retry_write(lambda : r.execute_command('AUTH', 'badpasswd'))
    # after that, we still got NOAUTH for get/set (return from redis-server)
    assert_equal(True, retry_read(lambda : r.ping()))
    assert_starts_with('NOAUTH', str(retry_write(lambda : r.set('k', 'v'))))
    assert_starts_with('NOAUTH', str(retry_read(lambda : r.get('k'))))

# NOTE: setup_and_wait was is/was used in developing the nosetests, see:
# https://github.com/idning/test-twemproxy/blob/master/Makefile#L17
def setup_and_wait():
    time.sleep(60*60)
