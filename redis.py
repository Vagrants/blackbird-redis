#!/usr/bin/env python
# -*- coding: utf-8 -*-
u"""
Send INFO and CONFIG GET to ZabbixServer.
"""

import re
import json
import sys
import socket
from telnetlib import Telnet
from blackbird.plugins import base


class ConcreteJob(base.JobBase):
    """
    This Class is called by "Executer"
    ConcreteJob is registerd as a job of Executer.
    """

    def __init__(self, options, queue=None, logger=None):
        super(ConcreteJob, self).__init__(options, queue, logger)

    def insert_queue(self, item):
        self.queue.put(item, block=False)
        self.logger.debug(
            'Inserted to queue redis.stat[{key}]:{value}'
            ''.format(key=item.key, value=item.value)
        )

    def looped_method(self):
        """
        Get stats data of redis by using telnet.
        """

        try:
            redis = RedisClient(host=self.options['host'],
                                port=self.options['port'],
                                db=self.options['db'],
                                timeout=self.options['timeout'],
                                auth=self.options['auth'])
        except RuntimeError as err:
            self.logger.warn(
                'connect failed. {0}'.format(err)
            )
            sys.exit(0)

        ignore = re.compile(r'^#')
        dbmatch = re.compile(r'^db\d+')
        lld_db = []
        # get INFO
        for line in redis.execute('INFO').split('\r\n'):
            if line == '' or re.match(ignore,line):
                continue

            [key, value] = line.split(':')

            # discovery
            # dbN:keys=N,expires=N,avg_ttl=N
            if re.match(dbmatch, key):
                lld_db.append(key)
                for lld_keys in value.split(','):
                    [lld_key, lld_value] = lld_keys.split('=')
                    item = RedisItem(
                        key="db,{0},{1}".format(key, lld_key),
                        value=lld_value,
                        host=self.options['hostname']
                    )
                    self.insert_queue(item)
                continue
            # discovery end

            item = RedisItem(
                key=key,
                value=value,
                host=self.options['hostname']
            )

            self.insert_queue(item)

        # discovery key
        item = RedisDiscoveryItem(
            key='db.LLD',
            value=lld_db,
            host=self.options['hostname']
        )
        self.insert_queue(item)

        # get CONFIG GET
        for cg in ['maxmemory', 'maxclients']:
            value = redis.execute('CONFIG', 'GET', cg)
            item = RedisItem(
                key=cg,
                value=value[1],
                host=self.options['hostname']
            )
            self.insert_queue(item)

        redis.close()
        sys.exit(0)


class RedisClient:
    """
    redis client library
    """
 
    def __init__(self, host, port, db, timeout, auth):
        try:
            self._connection = Telnet(host, port, timeout)
        except socket.error: 
            raise RuntimeError('Could not connect {host}:{port}'
                               ''.format(host=host,port=port))

        self._timeout = timeout
        if auth != 'None':
            if self.execute('AUTH', auth) != 'OK':
                raise RuntimeError('Could not AUTH')
        if db:
            if self.execute('SELECT', db) != 'OK':
                raise RuntimeError('Could not select db %d' % db)
 
    def execute(self, *request):
        self._sendline('*%d' % len(request))
        for arg in request:
            as_string = str(arg)
            self._sendline('$%d' % len(as_string))
            self._sendline(as_string)
        return self.read_command()
 
    def read_command(self):
        line = self._readline()
        prefix = line[0]
        rest = line[1:]
        if prefix == '+':
            return rest
        elif prefix == '-':
            raise RuntimeError('Redis error: %s' % rest)
        elif prefix == ':':
            return int(rest)
        elif prefix == '$':
            data_length = int(rest)
            if data_length == -1:
                return None
            else:
                data = ''
                first = True
                while len(data) < data_length:
                    if not first:
                        data += '\r\n'
                    data += self._readline()
                    first = False
                return data
        elif prefix == '*':
        # Recurse
            return list([self.read_command() for c in range(int(rest))])
        else:
            raise RuntimeError('Unknown response prefix "%s"' % prefix)
 
    def close(self):
        self._connection.close()
 
    def _sendline(self, line):
        self._connection.write(line)
        self._connection.write('\r\n')
 
    def _readline(self):
        line = self._connection.read_until('\r\n', self._timeout)[:-2]
        return line


class RedisItem(base.ItemBase):
    """
    Enqueued item.
    Take key(used by zabbix) and value as argument.
    """

    def __init__(self, key, value, host):
        super(RedisItem, self).__init__(key, value, host)

        self.__data = {}
        self._generate()

    @property
    def data(self):
        """Dequeued data."""

        return self.__data

    def _generate(self):
        """
        Convert to the following format:
        RedisItem(key='uptime', value='65535')
        {host:host, key:key1, value:value1, clock:clock}
        """

        self.__data['key'] = 'redis.stat[{0}]'.format(self.key)
        self.__data['value'] = self.value
        self.__data['host'] = self.host
        self.__data['clock'] = self.clock


class RedisDiscoveryItem(base.ItemBase):
    """
    Enqueued item.
    Take key(used by zabbix) and value as argument.
    """

    def __init__(self, key, value, host):
        super(RedisDiscoveryItem, self).__init__(key, value, host)

        self.__data = {}
        self._generate()

    @property
    def data(self):
        """Dequeued data."""

        return self.__data

    def _generate(self):
        """
        Convert to the following format:
        RedisItem(key='uptime', value='65535')
        {host:host, key:key1, value:value1, clock:clock}
        """

        self.__data['key'] = 'redis.stat.{0}'.format(self.key)
        self.__data['host'] = self.host
        self.__data['clock'] = self.clock

        value = {
            'data': [{'{#DB}': v} for v in self.value]
        }
        self.__data['value'] = json.dumps(value)


class Validator(base.ValidatorBase):
    """
    This class store information
    which is used by validation config file.
    """

    def __init__(self):
        self.__spec = None

    @property
    def spec(self):
        self.__spec = (
            "[{0}]".format(__name__),
            "host = ipaddress(default='127.0.0.1')",
            "port = integer(0, 65535, default=6379)",
            "db = integer(0, 65535, default=0)",
            "timeout = integer(default=10)",
            "hostname = string(default={0})".format(self.gethostname()),
        )
        return self.__spec
