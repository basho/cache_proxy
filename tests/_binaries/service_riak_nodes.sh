#!/bin/sh
TMP_BASE=/tmp/r/riak_devrel_
usage () {
    echo "Usage: $0 COMMAND NODE_NAME1 NODE_NAME2 .. NODE_NAMEn"
    echo "Commands:"
    echo "start - start all nodes"
    echo "stop  - stop all nodes"
    echo "ping  - ping all nodes"
}
riak_command () {
    RIAK_COMMAND=$1
    NODE=$2
    $TMP_BASE$NODE/bin/riak $RIAK_COMMAND
    RET=$?
    return $RET
}
riak_start_postwait () {
    NODE=$1
    echo 'perforning riak-admin test following start'
    RET=1
    while [ "$RET" != "0" ]; do
        $TMP_BASE$NODE/bin/riak-admin test
        RET=$?
        if [ "$RET" != "0" ]; then
            sleep 1
        fi
    done
}
redirect_command () {
    if [ "$SILENT" != "0" ]; then
        "$@" >/dev/null 2>&1
    else
        "$@"
    fi
}
RIAK_COMMAND=$1
shift
NODE_NAMES=$@
if [ -z SILENT ]; then
    SILENT=1
fi
if [ "$NODE_NAMES" = "" ]; then
    usage && exit 1
fi
for node_name in $NODE_NAMES; do
    redirect_command riak_command $RIAK_COMMAND $node_name
    RET=$?
done
if [ "$RIAK_COMMAND" = "start" ]; then
    for node_name in $NODE_NAMES; do
        #riak_start_postwait $node_name >/dev/null 2>&1
        redirect_command riak_start_postwait $node_name
        RET=$?
        if [ "$RET" = "1" ]; then
            break
        fi
    done
fi
exit "$RET"
