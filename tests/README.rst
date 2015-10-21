Python testing facilities for riak_rw_proxy, this test suite is based on
https://github.com/twitter/twemproxy tests which is based on
https://github.com/idning/redis-mgr

Memcached was removed as the cache proxy supported use case is Redis as the
cache layer, Riak as the persistent storage layer.

Redis commands were pared down to focus on key/value usage, ie EVAL and CRDT
functionality tests were marked __unsupported__ to cause nose to fail to match
the function name as a test.

Riak cluster _binaries are built from a devrel. The script
_binaries/create_riak_devrel_tarball.sh creates a tarball from the devrel
used for riak_test at ~/rt/riak/current/dev/dev1 , but this path may be
overridden by setting the environment variable RT_DEVREL_SRC .

usage
=====

1. install dependency::

    pip install nose
    pip install git+https://github.com/andymccurdy/redis-py.git@2.9.0
    pip install protobuf
    pip install cryptography
    pip install riak

    Note: "cryptography" require libffi-dev python-dev packages installed on
    your system. For Ubuntu run:
        sudo apt-get install libffi-dev python-dev

2. copy binarys to _binaries/::

    _binaries/
    |-- nutcracker
    |-- redis-cli
    |-- redis-server

2.a. roll a Riak devrel tarball
2.a.i. If you have an existing riak_test current setup, the default RT_DEVREL_SRC of
  ~/rt/riak/current/dev/dev1 will suffice.
2.a.ii. If you do not have an existing Riak devrel, see the Riak "Five-minute Install"
  http://docs.basho.com/riak/latest/quickstart/
2.a.iii. Set RT_DEVREL_SRC to the directory containing a Riak devrel.
2.a.iv. Execute _binaries/create_riak_devrel_tarball.sh .

3. run::

    $ nosetests -v
    test_del.test_multi_delete_on_readonly ... ok
    test_mget.test_mget ... ok

    ----------------------------------------------------------------------
    Ran 2 tests in 4.483s

    OK

4. add A case::

    cp tests/test_del.py tests/test_xxx.py
    vim tests/test_xxx.py



variables
=========
::

    export T_VERBOSE=9 will start nutcracker with '-v 9'  (default:4)
    export T_MBUF=512  will start nutcracker whit '-m 512' (default:521)
    export T_LARGE=10000 will test 10000 keys for mget/mset (default:1000)
    export T_RETRY_TIMES=5 will retry reads and writes (default:5)
    export T_RETRY_DELAY=0.1 will delay 0.1 seconds between retries (default:0.1)
    export T_PRIME_CONNECTION_DELAY=1.0 will delay 1.0 seconds between retries to prime the connection (default:1.0)
    export T_RIAK_MULTI=2 will execute 2 concurrent requests for multi tests (default:2)
    export T_RIAK_MANY=50 will execute 50 concurrent requests for manu tests (default:50)

T_LOGFILE:

- to put test log on stderr::

    export T_LOGFILE=-

- to put test log on t.log::

    export T_LOGFILE=t.log

  or::

    unset T_LOGFILE


notes
=====

- After all the tests. you may got a core because we have a case in test_signal
  which will send SEGV to nutcracker. Automatically removing coredump files is
  not performed.

