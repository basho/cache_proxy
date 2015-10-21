#!/usr/bin/env python
#coding: utf-8
#file   : server_modules.py
#author : ning
#date   : 2014-02-24 13:00:28

import os
import sys

from utils import *
import conf

from server_modules import *

class NutCracker(ServerBase):
    def __init__(self, host, port, path, cluster_name, masters, mbuf=512,
            verbose=5, is_redis=True, redis_auth=None, riak_cluster=None):
        ServerBase.__init__(self, 'nutcracker', host, port, path)

        self.masters = masters

        self.args['mbuf']        = mbuf
        self.args['verbose']     = verbose
        self.args['redis_auth']  = redis_auth
        self.args['conf']        = TT('$path/conf/nutcracker.conf', self.args)
        self.args['pidfile']     = TT('$path/log/nutcracker.pid', self.args)
        self.args['logfile']     = TT('$path/log/nutcracker.log', self.args)
        self.args['status_port'] = self.args['port'] + 1000

        self.args['startcmd'] = TTCMD('bin/nutcracker -d -c $conf -o $logfile \
                                       -p $pidfile -s $status_port            \
                                       -v $verbose -m $mbuf -i 1', self.args)
        self.args['runcmd']   = TTCMD('bin/nutcracker -d -c $conf -o $logfile \
                                       -p $pidfile -s $status_port', self.args)

        self.args['cluster_name']= cluster_name
        self.args['is_redis']= str(is_redis).lower()
        self.args['riak_cluster']= riak_cluster
        # HACK: await successful ping, otherwise getting requests ahead of the
        # service being up and running.
        self._alive()

    def _alive(self):
        return self._info_dict()

    def _gen_conf_section(self):
        template = '    - $host:$port:1 $server_name'
        cfg = '\n'.join([TT(template, master.args) for master in self.masters])
        return cfg

    def _gen_riak_conf_section(self):
        riak_cluster = self.args['riak_cluster']
        if riak_cluster == None:
            return ''

        server_template = '''
    - $host:$port:1
'''
        template = '''
  backend_type: riak
  backend_max_resend: 2
  backends: $backends
'''
        server_cfg = TT(server_template, {
            'host': riak_cluster.host(),
            'port': riak_cluster.port()
            })

        return TT(template, { 'backends': server_cfg })

    def _gen_conf(self):
        content = '''
$cluster_name:
  listen: 0.0.0.0:$port
  hash: fnv1a_64
  distribution: modula
  preconnect: true
  auto_eject_hosts: false
  redis: $is_redis
  backlog: 512
  timeout: 400
  client_connections: 0
  server_connections: 1
  server_retry_timeout: 2000
  server_failure_limit: 2
  server_ttl: 500ms
  servers:
'''
        if self.args['redis_auth']:
            content = content.replace('redis: $is_redis',
                    'redis: $is_redis\r\n  redis_auth: $redis_auth')
        content = TT(content, self.args)
        content = content + self._gen_conf_section()
        if self.args['riak_cluster'] != None:
            content = content + self._gen_riak_conf_section()
        return content

    def _pre_deploy(self):
        self.args['BINS'] = conf.BINARYS['NUTCRACKER_BINS']
        self._run(TT('cp $BINS $path/bin/', self.args))

        fout = open(TT('$path/conf/nutcracker.conf', self.args), 'w+')
        fout.write(self._gen_conf())
        fout.close()

    def version(self):
        #This is nutcracker-0.4.0
        s = self._run(TT('$BINS --version', self.args))
        return s.strip().replace('This is nutcracker-', '')

    def _info_dict(self):
        try:
            c = telnetlib.Telnet(self.args['host'], self.args['status_port'])
            ret = c.read_all()
            return json_decode(ret)
        except Exception, e:
            logging.debug('can not get _info_dict of nutcracker, \
                          [Exception: %s]' % (e, ))
            return None

    def reconfig(self, masters):
        self.masters = masters
        self.stop()
        self.deploy()
        self.start()
        logging.info('proxy %s:%s is updated' % (self.args['host'], self.args['port']))

    def logfile(self):
        return self.args['logfile']

    def cleanlog(self):
        cmd = TT("rm '$logfile'", self.args)
        self._run(cmd)

    def signal(self, signo):
        self.args['signo'] = signo
        cmd = TT("pkill -$signo -f '^$runcmd'", self.args)
        self._run(cmd)

    def reload(self):
        self.signal('USR1')

    def set_config(self, content):
        fout = open(TT('$path/conf/nutcracker.conf', self.args), 'w+')
        fout.write(content)
        fout.close()

        self.reload()

