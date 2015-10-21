#! /bin/sh
TMP_BASE=/tmp/r/riak_devrel_
usage () {
    echo "Usage: $0 NODE_NAME1 NODE_NAME2 .. NODE_NAMEn"
}
riak_ping () {
    NODE=$1
    echo "pinging $NODE"
    $TMP_BASE$NODE/bin/riak ping
}
riak_start () {
    NODE=$1
    echo "starting $NODE"
    riak_ping $NODE
    if [ $? = 0 ]; then
        return 0
    fi
    $TMP_BASE$NODE/bin/riak start
}
riak_join_plan () {
    HEAD_NODE=$1
    JOINING_NODE=$2
    if [ "$HEAD_NODE" = "$JOINING_NODE" ]; then
        return 0
    fi
    echo "joining $JOINING_NODE to $HEAD_NODE"
    $TMP_BASE$JOINING_NODE/bin/riak-admin cluster join $HEAD_NODE
}
riak_join_commit () {
    HEAD_NODE=$1
    $TMP_BASE$HEAD_NODE/bin/riak-admin cluster plan
    if [ $? != 0 ]; then
        echo "failed to plan cluster"
        return 1
    fi
    echo "committing cluster"
    $TMP_BASE$HEAD_NODE/bin/riak-admin cluster commit
}
NODE_NAMES=$@
if [ -z "$NODE_NAMES" ]; then
    usage && exit 1
fi
for node_name in $NODE_NAMES; do
    riak_start $node_name
done
HEAD_NODE=""
for node_name in $NODE_NAMES; do
    if [ -z $HEAD_NODE ]; then
        HEAD_NODE=$node_name
    fi
    riak_join_plan $HEAD_NODE $node_name
done
riak_join_commit $HEAD_NODE
