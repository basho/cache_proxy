#!/usr/bin/env python
#coding: utf-8
#file   : server_modules.py
#author : ning
#date   : 2014-02-24 13:00:28

import os
import sys

from utils import *
import conf

class ServerBase:
    '''
    Sub class should implement:
    _alive, _pre_deploy, status, and init self.args
    '''
    def __init__(self, name, host, port, path):
        self.args = {
            'name'      : name,
            'host'      : host,
            'port'      : port,
            'path'      : path,

            #startcmd and runcmd will used to generate the control script
            #used for the start cmd
            'startcmd'  : '',
            #process name you see in `ps -aux`, used this to generate stop cmd
            'runcmd'    : '',
            'logfile'   : '',
        }

    def __str__(self):
        return TT('[$name:$host:$port]', self.args)

    def deploy(self):
        logging.info('deploy %s' % self)
        self._run(TTCMD('mkdir -p $path/bin &&  \
                      mkdir -p $path/conf && \
                      mkdir -p $path/log &&  \
                      mkdir -p $path/data',
                self.args))

        self._pre_deploy()
        self._gen_control_script()

    def _gen_control_script(self):
        content = file(os.path.join(WORKDIR, 'conf/control.sh')).read()
        content = TT(content, self.args)

        control_filename = TT('${path}/${name}_control', self.args)

        fout = open(control_filename, 'w+')
        fout.write(content)
        fout.close()
        os.chmod(control_filename, 0755)

    def start(self):
        if self._alive():
            logging.warn('%s already running' %(self) )
            return

        logging.debug('starting %s' % self)
        t1 = time.time()
        sleeptime = .1

        cmd = TT("cd $path && ./${name}_control start", self.args)
        print 'Cmd is: ' + cmd;
        self._run(cmd)

        while not self._alive():
            lets_sleep(sleeptime)
            if sleeptime < 5:
                sleeptime *= 2
            else:
                sleeptime = 5
                logging.warn('%s still not alive' % self)

        t2 = time.time()
        logging.info('%s start ok in %.2f seconds' %(self, t2-t1) )
        if not self._alive():
            logging.debug('service is not reporting as alive after start')

    def stop(self):
        if not self._alive():
            logging.warn('%s already stop' %(self) )
            return

        cmd = TT("cd $path && ./${name}_control stop", self.args)
        self._run(cmd)

        t1 = time.time()
        while self._alive():
            lets_sleep()
        t2 = time.time()
        logging.info('%s stop ok in %.2f seconds' %(self, t2-t1) )

    def pid(self):
        cmd = TT("pgrep -f '^$runcmd'", self.args)
        return self._run(cmd)

    def status(self):
        logging.warn("status: not implement")

    def _alive(self):
        logging.warn("_alive: not implement")

    def _run(self, raw_cmd):
        ret = system(raw_cmd, logging.debug)
        logging.debug('return : [%d] [%s] ' % (len(ret), shorten(ret)) )
        return ret

    def clean(self):
        cmd = TT("rm -rf $path", self.args)
        self._run(cmd)

    def host(self):
        return self.args['host']

    def port(self):
        return self.args['port']

