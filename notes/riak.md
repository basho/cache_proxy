## Eventual Consistency

### Riak

Riak is a masterless distributed data store providing for availability over
consistency in the event of a network partition (AP). In order to do so, while
adhering to the laws of physics, data that is replicated from a primary node (A)
receiving the write to another primary node (B) may arrive successfully after a
client request to the primary node (B) which has not yet received the write.
During this tiny window of opportunity for divergence, a client may receive
stale data. However, eventually the primary nodes responsible for serving
a key will become consistent.

Riak provides for tunable consistency via bucket-properties as well as per-call
parameters. Using these tunable consistency options, a client can request to
only receive a response of a successful write after all of the primary nodes
have received the write. At this extreme, Riak is very nearly a consistent
over available system in the event of a network partition (CP).  For further
understanding of Riak configurable behaviors, see
http://basho.com/posts/technical/riaks-config-behaviors-part-4/

The Cache Proxy extends via configuration the eventual consistency parameters
which include the following:

    +---------------+------+--------------------------------------------------+
    | Key           | Type | Description                                      |
    +---------------+------+--------------------------------------------------+
    | r             | int  | Number of nodes to read, may contain fallbacks   |
    +---------------+------+--------------------------------------------------+
    | pr            | int  | Number of primary nodes to read                  |
    +---------------+------+--------------------------------------------------+
    | w             | int  | Number of nodes to write, may contain fallbacks  |
    +---------------+------+--------------------------------------------------+
    | pw            | int  | Number of primary nodes to write                 |
    +---------------+------+--------------------------------------------------+
    | n             | int  | Number of nodes to replicate to on write         |
    +---------------+------+--------------------------------------------------+
    | basic_quorum  | bit  | set to 1 to use a quorum based on n_val (n)      |
    +---------------+------+--------------------------------------------------+
    | sloppy_quorum | bit  | set to 1 to use a quorum that allows fallbacks   |
    +---------------+------+--------------------------------------------------+
    | notfound_ok   | bit  | set to 1 to treat notfound as ok                 |
    +---------------+------+--------------------------------------------------+


### Redis

Redis is a single node server that has been extended over several iterations
to provide fault tolerance via a relatively fast master-slave replication. Redis
replication has been extended via Redis Sentinel and outside means such as
HAProxy or dynamic DNS to support automated failover in the event that a master
crashes. The consistency guarantees following a network partition in these
roll-your-own Redis implementations vary based on the implementation which are
generally developed as an adjunct to a project, so have limited documentation,
so finding an answer to the guarantee is difficult, if at all possible. Redis
Cluster, which has been released but is not widely used, reduces the need for
outside agents by managing the master-slave relationships within the cluster,
but still at its core provides consistency guarantees that are based on
master-slave replication. For Redis Cluster consistency guarantees following a
network partition, see http://redis.io/topics/cluster-spec .

While it is possible to use Redis in a way that provides availability and
consistency guarantees that meet the requirements for an application, Redis
is an in-memory data store with data flushed to disk and/or replicated to
slaves. For datasets which have a large percentage of keys actively read this
characteristic is beneficial. However, the majority of datasets have large
amounts of data which is more cheaply stored on disk as the data is only
active for bursts, ie a specific customer and their health records will be
read, written, then not read again until either a report including their
data is prepared or the customer returns for follow up services.

The characteristics of Redis make it a relatively fast, acting well as a store
for cache. To this end, Redis provides expiration mechanisms including the
ability to set expiry during a write as well as the ability to set expiry
on a key without modifying the value stored at the key.

### Cache Proxy

The Cache Proxy utilizes Redis as a cache for values persisted in Riak. The
configuration option server_ttl is used to set expiry on Redis values during
the write operation. The result is that Redis becomes eventually consistent
with Riak with a window of inconsistency that has an upper bound of the
configured time-to-live (TTL) value.

Future support for writes through the Cache Proxy, will include
invalidating the Redis Cache is invalidated immediately via PEXPIRE so
subsequent reads will be consistent.

Future support for invalidating Redis cache on writes to Riak will likely
emerge as an enterprise feature.

## Riak Command Support

Riak is only exposed as a backend (BE), so is only integrated as a persistent
storage mechanism for a limited set of commands.

### String Commands

    +---------+----------------------------+------------------------------------+
    | Command | Format                     | Message Chain                      |
    +---------+----------------------------+------------------------------------+
    | GET     | GET key                    | BE GET -> FE SET w/ expiry         |
    +---------+----------------------------+------------------------------------+

## Sibling Resolution

Riak differs from Redis and Memcached in that Riak is first and foremost a
distributed data store. Riak is primarily an AP system meaning that availability
is guaranteed over consistency. In the event of a network partition, clients
on either side of the partition may write to Riak causing siblings, objects
that have divergence in their descendency. For a deeper explanation, see
http://docs.basho.com/riak/latest/dev/using/conflict-resolution/ .

While siblings are necessary for Riak, the Cache Proxy protocol which is based
on the Redis protocol, does not provide the necessary request nor the necessary
response components necessary to specify the ascendent or descendent  VClock
respectively. As such, sibling resolution within the Cache Proxy is necessary.

There are several possible sibling resolution strategies varying from not
resolving, presenting all of the siblings to the end-consumer of the data to
resolving to the last write, determined by the modified date on the Riak object.
While Riak cannot make a sane choice of server-side sibling resolution since the
sibling resolution strategy depends upon the use, the Cache Proxy has a
specific use, so can provide sibling resolution.

Of the possible sibling resolution strategies, the Cache Proxy currently
provides only resolution by last-modified date. Future releases will likely
provide configurable sibling resolution including the following strategies:

1. Dotted-Vector Version (DVV)
1. VClock
1. Last-Modified Date (LMD) aka Last-Write Wins (LWW)
