#!/bin/sh
# Create a tarball from an existing Riak devrel.
#
# The resulting riak_devrel.tar.gz is used by the nosetests to ensure a fresh Riak setup per test.
#
# Set RT_DEVREL_SRC to the base directory of a devrel. The default is to use the riak_test (r_t)
# current setup's first node, so ~/rt/riak/current/dev/dev1.
# 
# The tarball is rolled by the following steps:
# 1. copy RT_DEVREL_SRC to /tmp/r/riak_devrel
# 2. remove the contents of the data directory
# 3. remove the contents of the log directory
# 4. roll the tarball
# 5. clean up - remove the directory /tmp/r/riak_devrel

DIR=$(cd "$(dirname $0)" && pwd)
if [ -z $RT_DEVREL_SRC ]; then
    RT_DEVREL_SRC=~/rt/riak/current/dev/dev1
fi
TMP_DEVREL_DEST=/tmp/r/riak_devrel
DEVREL_TARBALL="$DIR/riak_devrel.tar.gz"

rm $DEVREL_TARBALL

mkdir -p $TMP_DEVREL_DEST >/dev/null 2>&1

cp -r $RT_DEVREL_SRC/* $TMP_DEVREL_DEST
if [ -d "$TMP_DEVREL_DEST/data" ]; then
    rm -rf $TMP_DEVREL_DEST/data
fi
mkdir $TMP_DEVREL_DEST/data
if [ -d "$TMP_DEVREL_DEST/log" ]; then
    rm -rf $TMP_DEVREL_DEST/log
fi
mkdir $TMP_DEVREL_DEST/log
cd $(dirname $TMP_DEVREL_DEST)
tar -czvf $DEVREL_TARBALL $(basename $TMP_DEVREL_DEST) >/dev/null 2>&1
rm -rf $TMP_DEVREL_DEST

