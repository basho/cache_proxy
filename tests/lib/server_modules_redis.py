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

class RedisServer(ServerBase):
    def __init__(self, host, port, path, cluster_name, server_name, auth = None):
        ServerBase.__init__(self, 'redis', host, port, path)

        self.args['startcmd']     = TT('bin/redis-server conf/redis.conf', self.args)
        self.args['runcmd']       = TT('redis-server \*:$port', self.args)
        self.args['conf']         = TT('$path/conf/redis.conf', self.args)
        self.args['pidfile']      = TT('$path/log/redis.pid', self.args)
        self.args['logfile']      = TT('$path/log/redis.log', self.args)
        self.args['dir']          = TT('$path/data', self.args)
        self.args['REDIS_CLI']    = conf.BINARYS['REDIS_CLI']

        self.args['cluster_name'] = cluster_name
        self.args['server_name']  = server_name
        self.args['auth']         = auth
        # HACK: await successful ping, otherwise getting requests ahead of the
        # service being up and running.
        self._alive()

    def _info_dict(self):
        cmd = TT('$REDIS_CLI -h $host -p $port INFO', self.args)
        if self.args['auth']:
            cmd = TT('$REDIS_CLI -h $host -p $port -a $auth INFO', self.args)
        info = self._run(cmd)

        info = [line.split(':', 1) for line in info.split('\r\n')
                if not line.startswith('#')]
        info = [i for i in info if len(i) > 1]
        return defaultdict(str, info) #this is a defaultdict, be Notice

    def _ping(self):
        cmd = TT('$REDIS_CLI -h $host -p $port PING', self.args)
        if self.args['auth']:
            cmd = TT('$REDIS_CLI -h $host -p $port -a $auth PING', self.args)
        return self._run(cmd)

    def _alive(self):
        return strstr(self._ping(), 'PONG')

    def _gen_conf(self):
        content = file(os.path.join(WORKDIR, 'conf/redis.conf')).read()
        content = TT(content, self.args)
        if self.args['auth']:
            content += '\r\nrequirepass %s' % self.args['auth']
        return content

    def _pre_deploy(self):
        self.args['BINS'] = conf.BINARYS['REDIS_SERVER_BINS']
        self._run(TT('cp $BINS $path/bin/', self.args))

        fout = open(TT('$path/conf/redis.conf', self.args), 'w+')
        fout.write(self._gen_conf())
        fout.close()

    def status(self):
        uptime = self._info_dict()['uptime_in_seconds']
        if uptime:
            logging.info('%s uptime %s seconds' % (self, uptime))
        else:
            logging.error('%s is down' % self)

    def isslaveof(self, master_host, master_port):
        info = self._info_dict()
        if info['master_host'] == master_host and \
           int(info['master_port']) == master_port:
            logging.debug('already slave of %s:%s' % (master_host, master_port))
            return True

    def slaveof(self, master_host, master_port):
        cmd = 'SLAVEOF %s %s' % (master_host, master_port)
        return self.rediscmd(cmd)

    def rediscmd(self, cmd):
        args = copy.deepcopy(self.args)
        args['cmd'] = cmd
        cmd = TT('$REDIS_CLI -h $host -p $port $cmd', args)
        logging.info('%s %s' % (self, cmd))
        return self._run(cmd)

