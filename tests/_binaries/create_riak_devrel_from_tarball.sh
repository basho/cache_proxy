#!/bin/sh
usage () {
    echo "Usage: $0 NODE_NAME PB_PORT"
}
NODE_NAME=$1
PB_PORT=$2
if [ "$NODE_NAME" = "" ] || [ "$PB_PORT" = "" ]; then
    usage && exit 1
fi
DIR=$(cd "$(dirname $0)" && pwd)
DEVREL_TARBALL=$DIR/riak_devrel.tar.gz
TMP_DEVREL_DEST=/tmp/r/riak_devrel_$NODE_NAME
if [ -d $TMP_DEVREL_DEST ]; then
    rm -rf $TMP_DEVREL_DEST
fi
tar xzf $DEVREL_TARBALL -C `dirname $TMP_DEVREL_DEST`
mv `dirname $TMP_DEVREL_DEST`/riak_devrel $TMP_DEVREL_DEST
# change riak.conf
sed -ibak "s/nodename = .*/nodename = $NODE_NAME@127.0.0.1/" $TMP_DEVREL_DEST/etc/riak.conf
sed -ibak "s/listener.protobuf.internal = .*/listener.protobuf.internal = 127.0.0.1:$PB_PORT/" $TMP_DEVREL_DEST/etc/riak.conf
sed -ibak "s/listener.http.internal = .*/listener.http.internal = 127.0.0.1:1$PB_PORT/" $TMP_DEVREL_DEST/etc/riak.conf
sed -ibak "s/search.solr.port = .*/search.solr.port = 2$PB_PORT/" $TMP_DEVREL_DEST/etc/riak.conf
sed -ibak "s/search.solr.jmx_port = .*/search.solr.jmx_port = 3$PB_PORT/" $TMP_DEVREL_DEST/etc/riak.conf
# change advanced.config, if present
if [ -e "$TMP_DEVREL_DEST/etc/advanced.config" ]; then
    sed -ibak "s/{cluster_mgr, {.*}/{cluster_mgr, {\"127.0.0.1\", 4$PB_PORT}}/" $TMP_DEVREL_DEST/etc/advanced.config
fi
# change snmp configs
if [ -e "$TMP_DEVREL_DEST/etc/snmp/agent/conf/agent.conf" ]; then
    sed -ibak "s/{intAgentUDPPort, .*}/{intAgentUDPPort, 5$PB_PORT }/" $TMP_DEVREL_DEST/etc/snmp/agent/conf/agent.conf
fi
# change sysctl inputs
# handoff port
sed -ibak "s/{default, 10019.*}/{default, 6$PB_PORT}/" $TMP_DEVREL_DEST/lib/12-riak_core.schema
