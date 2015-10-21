#!/usr/bin/env python
from common import *
from pprint import pprint

def get_conn():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((nc.host(), nc.port()))
    s.settimeout(.3)
    return s

def _test(req, resp, sleep=0):
    s = get_conn()

    for i in req:
        s.sendall(i)
        time.sleep(sleep)

    s.settimeout(.3)

    data = s.recv(10000)
    assert(data == resp)

def test_slow():
    req = '*1\r\n$4\r\nPING\r\n'
    resp = '+PONG\r\n'

    if large > 1000:
        sleep = 1
    else:
        sleep = .1

    retry_read(lambda : _test(req, resp, sleep))

def test_pingpong():
    req = '*1\r\n$4\r\nPING\r\n'
    resp = '+PONG\r\n'
    retry_read(lambda : _test(req, resp))

def test_quit():
    if nc.version() < '1.0.0':
        return
    req = '*1\r\n$4\r\nQUIT\r\n'
    resp = '+OK\r\n'
    retry_read(lambda : _test(req, resp))

def _quit_without_recv(req, resp):
    s = get_conn()

    s.sendall(req)
    s.close()
    info = nc._info_dict()
    #pprint(info)
    assert(info['ntest']['client_err'] == 1)

def test_quit_without_recv():
    if nc.version() < '1.0.0':
        return
    req = '*1\r\n$4\r\nQUIT\r\n'
    resp = '+OK\r\n'
    retry_read(lambda : _quit_without_recv(req, resp))

def _test_bad(req):
    s = get_conn()

    s.sendall(req)
    data = s.recv(10000)
    print data

    assert('' == s.recv(1000))  # peer is closed

def test_badreq():
    reqs = [
        # '*1\r\n$3\r\nPING\r\n',
        '\r\n',
        # '*3abcdefg\r\n',
        '*3\r\n*abcde\r\n',

        # '*4\r\n$4\r\nMSET\r\n$1\r\nA\r\n$1\r\nA\r\n$1\r\nA\r\n',
        # '*2\r\n$4\r\nMSET\r\n$1\r\nA\r\n',
        # '*3\r\n$abcde\r\n',
        # '*3\r\n$3abcde\r\n',
        # '*3\r\n$3\r\nabcde\r\n',
    ]

    for req in reqs:
        retry_read(lambda : _test_bad(req))


def _wrong_argc():
    s = get_conn()

    s.sendall('*1\r\n$3\r\nGET\r\n')
    try:
        assert('' == s.recv(1000))  # peer is closed
    except socket.timeout:
        pass

def test_wrong_argc():
    retry_read(lambda : _wrong_argc())
