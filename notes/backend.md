## Backend Types Supported

### Redis

In order to expedite development of Cache Proxy commands, the backend server
type Redis was created. The Redis MONITOR command provides visibility without
network sniffing, additional logging, or other means of determining what
commands the Cache Proxy is emitting to the frontend as well as backend servers.

### Cache Proxy

Since the Cache Proxy speaks the Redis protocol, the backend server type Redis
allows for chaining Cache Proxy servers. While the use of chaining may not be
necessary, the ability to chain servers allows for additional implementation
options, such as a Cache Proxy and Redis frontend server per client application
with a very short TTL, acting as a level 2 (L2) cache, with multiple backend
Cache Proxy servers with a relatively longer TTL, acting as a level 1 cache. The
top few benefits from such an implementation follow:

 * reduced number of Cache Proxy servers to invalidate on a write to distributed
   backend, only L1 cache servers require invalidation.
 * reduced latency with little increase in inconsist state of L2 cache.

### Riak

Riak is only available as a backend server type. The original reason for
forking Twemproxy to create a Cache as a Service implementation was to provide
benefits to both Redis and Riak users.

Benefits to Riak users follow:

 * Reduced latency by approximately 5X for data fitting an access pattern that
   results in an 80% cache hit ratio. For example, for data which is accessed
   infrequently, but accessed in bursts, a cache hit ratio of greater than
   80% is typical of applications which are stateless, ie web applications
   as well as rich-client applications that may assume a connection to the
   data tier.
 * Reduced code complexity with reduced chance of Riak anti patterns.
   Cache Proxy provides sibling resolution, avoids sibling explosion, reduces
   inconsistent time window through a configurable cache TTL, and provides the
   ease of access and broad client language support of the Redis community.
 * Increased cache memory available via Redis presharding with memory
   consumption statistics available via the underlying Redis instances.
 * Continued operational simplicity as Redis and Cache Proxy supervision
   is easily implemented either via the recommended Basho Data Platform (BDP)
   or via broadly available tooling from the Redis and Twemproxy communities.

Benefits to Redis users follow:

 * Increased data safety and operational simplicity as the masterless Riak
   cluster implementation provides configurable consistency guarantees while
   providing a system that is available during network partitions.
 * Reduced total cost of ownership with Riak acting as the persistent data
   store. The in-memory requirement of Redis is reduced to only the working set
   of data. Note: for small data sets this is less beneficial.
 * Reduced development effort for clients who already have a significant amount
   of familiarity and/or code that is being upgraded from using Redis as the
   persistent data store to using Riak via the Cache Proxy via the Redis
   protocol using existing client libraries.

## Future Benefits
### CRDT Support

Redis, which is more of a data structure server than a simple cache server,
provides similar Convergent Replicated Data Type (CRDT) objects and commands.
Since NoSQL data stores do not automatically index all records via primary key
nor provide Data Definition Language (DDL) to automate additional indexing
including foreign key indexes, sets are often used in both Redis and Riak
powered applications. In order to promote and encapsulate the caching of this
pattern, the Cache Proxy will provide support for additional data types. The
priority of development of additional data types will continue to be driven
by the following preferences:

 * broad range of client use cases
 * difficulty in achieving the CRDT without support from Cache Proxy, ie
   Sorted Sets which are available in Redis as a single CRDT but not in Riak
 * clarity in mapping between Redis and Riak CRDT

### Extended Cache TTL

For systems that notify or provide an event queue on writes to Riak, such as will be
provided by the Basho Data Platform via the Monotonic Event Queue (MEQ), the Cache TTL can
be extended, even up to several days, increasing the cache hit ratio for data which is
less frequently accessed, yet whose clients have an extreme low latency requirement, while
also reducing the time window that cached data is inconsistent. On writes to Riak, a
subscriber to the MEQ can pull the affected keys and issue cache invalidation messages
(PEXPIRE).

## Message Mapping
### String Commands

    +---------+----------------------------+------------------------------------+
    | Command | Format                     | Message Chain                      |
    +---------+----------------------------+------------------------------------+
    | GET     | GET key                    | BE GET -> FE SET w/ expiry         |
    +---------+----------------------------+------------------------------------+

## Source Files Affected

 * hashkit/nc_hashkit.h
  * backend parameter added to <distribution_type>_update
 * hashkit/nc_ketama.c
  * backend parameter added to <distribution_type>_update
 * hashkit/nc_modula.c
  * backend parameter added to <distribution_type>_update
 * hashkit/nc_random.c
  * backend parameter added to <distribution_type>_update
 * nc_backend.[ch]
 * nc_conf.[ch]
  * 
 * nc_connection.h
 * nc_log.h
 * nc_message.[ch]
 * nc_proxy.c
 * nc_request.c
 * nc_response.c
 * nc_server.[ch]
 * nc_stats.[ch]
 * proto/nc_memcache.c
 * proto/nc_redis.c
 * proto/nc_riak.c
 * proto/riak.pb-c.c
 * proto/riak.pb-c.h

## Internal Message Flow

1. When the message is acquired from the free message pool via msg_get(),
msg->backend_process is set to backend_process_rsp().
1. When the response filter successfully dequeues a non-empty response from the frontend
   server and the corresponding peer message (request), msg->backend_process() is called.
1. If the backend process creates additional requests, ie a request to the backend
   server, the frontend response is not forwarded or swallowed as is typical in
   rsp_filter(), so such message handling, if necessary should be handled within
   the backend process.

