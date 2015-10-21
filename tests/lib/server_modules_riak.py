#!/usr/bin/env python
#coding: utf-8
#file   : server_modules.py
#author : ning
#date   : 2014-02-24 13:00:28

import os
import sys

from utils import *
import conf

# NOTE: did not derive from ServerBase as the Riak cluster is easier to manage
# as duck equivalent instead of actually inheritence chain equivalence.

class RiakCluster:
    # node_name_ports is an array of tuples of (node_name, port) where port is
    # the protobuf port from which all other ports ascend, ie http is pb_port
    # + 10000 .
    def __init__(self, node_name_ports):
        self.args = {
                'name'             : 'riak',
                'node_name_ports'  : node_name_ports,
                }

    def __str__(self):
        return TT('[$name:$node_name_ports]', self.args)

    def deploy(self):
        logging.info('deploy %s' % self)
        for (node_name, port) in self.node_name_ports():
            self._run(TT('./_binaries/create_riak_devrel_from_tarball.sh $node_name $port', {'node_name': node_name, 'port': port}))

    def start(self):
        if self._alive():
            logging.warn('%s already start' % (self))
            return

        t1 = time.time()
        max_wait = 60
        while not self._alive():
            if self._start():
                break
            if time.time() - t1 > max_wait:
                break
        t2 = time.time()
        logging.info('%s start ok in %.2f seconds' % (self, t2 - t1))

    def _start(self):
        ret = self._cluster_command('./_binaries/service_riak_nodes.sh start')
        if len(self.node_name_ports()) > 1:
            self._cluster_command('./_binaries/create_riak_cluster.sh')
        return 0 == ret

    def stop(self):
        if not self._alive():
            logging.warn('%s already stop' % (self))
            return

        t1 = time.time()
        max_wait = 60
        while self._alive():
            if self._stop():
                break
            if time.time() - t1 > max_wait:
                break
        t2 = time.time()
        logging.info('%s stop ok in %.2f seconds' %(self, t2 - t1))

    def _stop(self):
        if len(self.node_name_ports()) > 1:
            self._cluster_command('./_binaries/teardown_riak_cluster.sh')
        ret = self._cluster_command('./_binaries/service_riak_nodes.sh stop')
        return 0 == ret

    def _cluster_command(self, command_script):
        cmd_args = {
                'command_script': command_script,
                'node_names': ' '.join(self.node_names())
                }
        return self._run(TT('$command_script $node_names', cmd_args))

    def node_name_ports(self):
        return self.args['node_name_ports']

    def node_names(self):
        return map(lambda (nn, p): nn, self.node_name_ports())

    def pid(self):
        logging.warn('pid: not implemented')

    def status(self):
        logging.warn('status: not implemented')

    def _alive(self):
        try:
            return self.__alive()
        except: # TimeoutError:
            return False

    def __alive(self):
       ret = self._cluster_command('./_binaries/service_riak_nodes.sh ping')
       return 0 == ret

    def _run(self, raw_cmd):
        logging.debug('running: %s' % raw_cmd)
        ret = os.system(raw_cmd)
        logging.debug('[%d] %s' % (ret, raw_cmd))
        return ret

    def clean(self):
        pass

    def base_dir(self):
        return '/tmp/r/'

    def host(self):
        return '127.0.0.1'

    def port(self):
        return self._pb_port(self.node_names()[0])

    def _devrel_path(self, node_name):
        return '%s/riak_devrel_%s' % (self.base_dir(), node_name)

    def _pb_port(self, node_name):
        # could easily read the arg, but checking the configured value is better
        riak_conf_path = '%s/etc/riak.conf' % (self._devrel_path(node_name))
        for conf_line in open(riak_conf_path, 'r'):
            if 0 <= conf_line.find('listener.protobuf.internal'):
                return int(conf_line.split(':')[-1])

        return -1
