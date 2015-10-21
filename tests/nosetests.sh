#!/bin/sh
if [ -z "$T_VERBOSE" ]; then
    verbose=""
else
    verbose="-vvv"
fi

if [ -z "$top_srcdir" ]; then
    top_srcdir="$(dirname "$0")/.."
fi

# If no tests were selected, select all of them
if [ $# -eq 0 ]; then
    set -- "${top_srcdir}"/tests/test_*
fi

if [ ! -e "${top_srcdir}/tests/_binaries/riak_devrel.tar.gz" ]; then
    echo "Riak devrel tarball does not exist, see tests/README.rst"
    exit 1
fi

exec nosetests "${verbose}" -a \!acceptance,\!slow "$@"
