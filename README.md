# cache_proxy (BDP Cache Proxy Open Source Software (OSS) Edition) #
## Purpose ##
BDP Cache Proxy, aka **nutcracker'** (until someone wins the contest to name the service) is a
fast and lightweight proxy for [redis](http://redis.io/) protocol. It was built primarily to
reduce Riak KV mean response times, especially for immutable data, as well as reduce client
cost and implementation time while increasing flexibility in implementations by supporting with
little effort a horizontally scalable distributed cache and persistent data store architecture.

## Lineage ##
BDP Cache Proxy is based on twemproxy, with the following high-level differences:
1. Cache strategy implementations are moved from the client into the Cache Proxy service.
2. Upstream persistent data stores, aka 'Backend' servers were necessarily added.
2.a. Riak KV is the one and only persistent data store supported.
2.b. Redis may also be configured as a Backend server, useful only for development/testing.

As mentioned earlier, nutcracker' is subject to name change. All processes dependent upon the
executable's name are thus subject to change. For typical BDP use, all such changes, ie
BDP Service Manager (SM) integration, will be encapsulated in BDP.

See README.twemproxy.md for fuller twemproxy coverage, but consider any topic not covered here to
not be supported use of the BDP Cache Proxy.

## Dependencies ##
BDP Cache Proxy depends upon libprotobuf-c to communicate with Riak KV.  If you already have
libprotobuf-c present, skip to Build.

### Build libprotobuf-c ###
Refer to https://github.com/protobuf-c/protobuf-c

```bash
git clone https://github.com/protobuf-c/protobuf-c
cd protobuf-c
./autogen.sh && ./configure && make
make install
```

## Build ##
To build BDP Cache Proxy from source with _debug logs enabled_ and _assertions enabled_:

    $ git clone git@github.com:basho/cache_proxy.git
    $ cd cache_proxy
    $ autoreconf -fvi
    $ ./configure --enable-debug=full --with-protobuf-c=/usr/local
    $ make
    $ src/nutcracker -h

A quick checklist:

+ Use newer version of gcc (older version of gcc has problems)
+ Use CFLAGS="-O1" ./configure && make
+ Use CFLAGS="-O3 -fno-strict-aliasing" ./configure && make
+ `autoreconf -fvi && ./configure` needs `automake` and `libtool` to be installed

## Features ##

+ Fast.
+ Lightweight.
+ Maintains persistent server connections.
+ Keeps connection count on the caching and persistent data store servers low.
+ Enables pipelining of requests and responses.
+ Supports proxying to multiple servers.
+ Supports multiple server pools simultaneously.
+ Shard data automatically across multiple servers.
+ Easy configuration of server pools through a YAML file.
+ Supports multiple hashing modes including consistent hashing and distribution.
+ Can be configured to disable nodes on failures.
+ Observability via stats exposed on the stats monitoring port.
+ Works with Centos, Linux, *BSD, OS X.

## Help ##

    Usage: nutcracker [-?hVdDt] [-v verbosity level] [-o output file]
                      [-c conf file] [-s stats port] [-a stats addr]
                      [-i stats interval] [-p pid file] [-m mbuf size]

    Options:
      -h, --help             : this help
      -V, --version          : show version and exit
      -t, --test-conf        : test configuration for syntax errors and exit
      -d, --daemonize        : run as a daemon
      -D, --describe-stats   : print stats description and exit
      -v, --verbose=N        : set logging level (default: 5, min: 0, max: 11)
      -o, --output=S         : set logging file (default: stderr)
      -c, --conf-file=S      : set configuration file (default: conf/nutcracker.yml)
      -s, --stats-port=N     : set stats monitoring port (default: 22222)
      -a, --stats-addr=S     : set stats monitoring ip (default: 0.0.0.0)
      -i, --stats-interval=N : set stats aggregation interval in msec (default: 30000 msec)
      -p, --pid-file=S       : set pid file (default: off)
      -m, --mbuf-size=N      : set size of mbuf chunk in bytes (default: 16384 bytes)

## Configuration ##
BDP Cache Proxy can be configured through a YAML file specified by the -c or --conf-file
command-line argument on process start. The configuration file is used to specify the server
pools and the servers within each pool that the BDP Cache Proxy manages. The configuration files
supports the following keys:

+ **listen**: The listening address and port (name:port or ip:port) or an absolute path to sock file (e.g. /var/run/nutcracker.sock) for this server pool.
+ **hash**: The name of the hash function. Possible values are:
 + one_at_a_time
 + md5
 + crc16
 + crc32 (crc32 implementation compatible with [libmemcached](http://libmemcached.org/))
 + crc32a (correct crc32 implementation as per the spec)
 + fnv1_64
 + fnv1a_64
 + fnv1_32
 + fnv1a_32
 + hsieh
 + murmur
 + jenkins
+ **hash_tag**: A two character string that specifies the part of the key used for hashing. Eg "{}" or "$$". [Hash tag](notes/recommendation.md#hash-tags) enable mapping different keys to the same server as long as the part of the key within the tag is the same.
+ **distribution**: The key distribution mode. Possible values are:
 + ketama
 + modula
 + random
+ **timeout**: The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. By default, we wait indefinitely.
+ **backlog**: The TCP backlog argument. Defaults to 512.
+ **preconnect**: A boolean value that controls if the Cache Proxy should preconnect to all the
servers in this pool on process start. Defaults to false.
+ **redis**: A boolean value that controls if a server pool speaks redis or memcached protocol.
Defaults to false.
+ **redis_auth**: Authenticate to the Redis server on connect.
+ **server_connections**: The maximum number of connections that can be opened to each server. By default, we open at most 1 server connection.
+ **auto_eject_hosts**: A boolean value that controls if server should be ejected temporarily when it fails consecutively server_failure_limit times. See [liveness recommendations](notes/recommendation.md#liveness) for information. Defaults to false.
+ **server_retry_timeout**: The timeout value in msec to wait for before retrying on a temporarily ejected server, when auto_eject_host is set to true. Defaults to 30000 msec.
+ **server_failure_limit**: The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject_host is set to true. Defaults to 2.
+ **server_ttl**: Cache time-to-live (TTL), specified in unit format, ie 15s for 15 seconds.
+ **servers**: A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool.
+ **backend_type**: riak (supported) or redis (useful only for development/testing)
+ **backend_max_resend**: positive integer, the maximum number of times to attempt resending a
message to the backend server.
+ **backends**: A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool.

For example, see the configuration file in [conf/backend_test.yml](conf/backend_test.yml).

Finally, to make writing a syntactically correct configuration file easier, BDP Cache Proxy
provides a command-line argument -t or --test-conf that can be used to test the YAML
configuration file for any syntax error.

## Observability

Observability in BDP Cache Proxy is through logs and stats.

BDP Cache Proxy exposes stats at the granularity of server pool and servers per pool through the stats monitoring port. The stats are essentially JSON formatted key-value pairs, with the keys corresponding to counter names. By default stats are exposed on port 22222 and aggregated every 30 seconds. Both these values can be configured on program start using the -c or --conf-file and -i or --stats-interval command-line arguments respectively. You can print the description of all stats exported by  using the -D or --describe-stats command-line argument.

    $ nutcracker --describe-stats

    pool stats:
      client_eof          "# eof on client connections"
      client_err          "# errors on client connections"
      client_connections  "# active client connections"
      server_ejects       "# times backend server was ejected"
      forward_error       "# times we encountered a forwarding error"
      fragments           "# fragments created from a multi-vector request"

    server stats:
      server_eof          "# eof on server connections"
      server_err          "# errors on server connections"
      server_timedout     "# timeouts on server connections"
      server_connections  "# active server connections"
      requests            "# requests"
      request_bytes       "total request bytes"
      responses           "# responses"
      response_bytes      "total response bytes"
      in_queue            "# requests in incoming queue"
      in_queue_bytes      "current request bytes in incoming queue"
      out_queue           "# requests in outgoing queue"
      out_queue_bytes     "current request bytes in outgoing queue"

Logging in BDP Cache Proxy is only available when built with logging enabled. By default logs are written to stderr. BDP Cache Proxy can also be configured to write logs to a specific file through the -o or --output command-line argument. On a running BDP Cache Proxy, we can turn log levels up and down by sending it SIGTTIN and SIGTTOU signals respectively and reopen log files by sending it SIGHUP signal.

## Pipelining

BDP Cache Proxy enables proxying multiple client connections onto one or few server connections. This architectural setup makes it ideal for pipelining requests and responses and hence saving on the round trip time.

For example, if BDP Cache Proxy is proxying three client connections onto a single server and we get requests - 'get key\r\n', 'set key 0 0 3\r\nval\r\n' and 'delete key\r\n' on these three connections respectively, BDP Cache Proxy would try to batch these requests and send them as a single message onto the server connection as 'get key\r\nset key 0 0 3\r\nval\r\ndelete key\r\n'.

Pipelining is the reason why BDP Cache Proxy ends up doing better in terms of throughput even though it introduces an extra hop between the client and server.

## Deployment

If you are deploying BDP Cache Proxy in production, you might consider reading through the [recommendation document](notes/recommendation.md) to understand the parameters you could tune in BDP Cache Proxy to run it efficiently in the production environment.  However, by installing BDP Cache Proxy as part of the Basho Data Platform, these recommendations have already been heeded, yielding the best performance and reducing the total cost of ownership.

## Packages

### Centos
#TODO: create Centos (RH, rpm) package

### Ubuntu
#TODO: create Ubuntu (Debian, deb) package

## Issues and Support

Have a bug or a question? Please create an issue here on GitHub!

https://github.com/basho/cache_proxy/issues

## Contribution ##
Thank you to all of our [contributors](https://github.com/basho/cache_proxy/graphs/contributors)!

## License

© 2015 Basho Technologies

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
