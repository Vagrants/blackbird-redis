#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=too-many-arguments,missing-docstring,too-few-public-methods
"""
Send INFO and CONFIG GET to ZabbixServer.
"""

__VERSION__ = '0.1.2'

import re
import socket
from datetime import datetime
from telnetlib import Telnet
from blackbird.plugins import base


class ConcreteJob(base.JobBase):
    """
    This Class is called by "Executor"
    Get redis's info,
    and send to specified zabbix server.
    """

    def __init__(self, options, queue=None, logger=None):
        super(ConcreteJob, self).__init__(options, queue, logger)

    def build_items(self):
        """
        main loop
        """

        # ping item
        self._ping()

        redis = self._connect()

        # get stats by INFO
        self._get_stats(redis)

        if 'response_check_key' in self.options:
            # get response time by SET
            self._response_set(redis)

            # get response time by GET
            self._response_get(redis)

        # bye :-)
        redis.close()

    def build_discovery_items(self):
        """
        main loop for lld
        """

        redis = self._connect()

        # get dbname for lld
        self._get_lld_stats(redis)

        # bye :-)
        redis.close()

    def _enqueue(self, key, value):

        item = RedisItem(
            key=key,
            value=value,
            host=self.options['hostname']
        )
        self.queue.put(item, block=False)
        self.logger.debug(
            'Inserted to queue {key}:{value}'
            ''.format(key=key, value=value)
        )

    def _connect(self):
        try:
            redis = RedisClient(host=self.options['host'],
                                port=self.options['port'],
                                db=self.options['db'],
                                timeout=self.options['timeout'],
                                auth=self.options['auth'])
        except RuntimeError as err:
            raise base.BlackbirdPluginError(
                'connect failed. {0}'.format(err)
            )

        return redis

    def _ping(self):
        """
        send ping item
        """
        self._enqueue('blackbird.redis.ping', 1)
        self._enqueue('blackbird.redis.version', __VERSION__)

    def _get_stats(self, redis):
        """
        Get stats data of redis by using telnet.
        """

        ignore = re.compile(r'^#')
        dbmatch = re.compile(r'^db\d+')

        # get INFO
        for line in redis.execute('INFO').split('\r\n'):
            if line == '' or re.match(ignore, line):
                continue

            key, value = line.split(':')

            if re.match(dbmatch, key):
                # db stats
                for db_keys in value.split(','):
                    db_key, db_value = db_keys.split('=')
                    self._enqueue(
                        'redis.stat[db,{0},{1}]'.format(key, db_key),
                        db_value
                    )
            else:
                # normal key value stats
                self._enqueue('redis.stat[{key}]'.format(key=key), value)

        # get CONFIG GET
        for config_get in ['maxmemory', 'maxclients']:
            value = redis.execute('CONFIG', 'GET', config_get)[1]
            self._enqueue('redis.stat[{key}]'.format(key=config_get), value)

    def _get_lld_stats(self, redis):
        """
        Get lld stats data of redis by using telnet.
        """

        lld_db = []

        # discovery
        # dbN:keys=N,expires=N,avg_ttl=N
        dbmatch = re.compile(r'^(db\d+):')

        # get dbN
        for line in redis.execute('INFO').split('\r\n'):
            if re.match(dbmatch, line):
                key, _ = line.split(':')
                lld_db.append(key)

        if len(lld_db) > 0:
            item = base.DiscoveryItem(
                key='redis.db.LLD',
                value=[{'{#DB}': dbname} for dbname in lld_db],
                host=self.options['hostname']
            )
            self.queue.put(item, block=False)

    def _response_set(self, redis):
        dummy_value = datetime.now().strftime('%Y%m%d%H%M%S')
        with base.Timer() as timer:
            redis.execute('SET', '__zabbix_check', dummy_value)
        self._enqueue('redis.stat[set_response]', timer.sec)

    def _response_get(self, redis):
        with base.Timer() as timer:
            redis.execute('GET', '__zabbix_check')
        self._enqueue('redis.stat[get_response]', timer.sec)


class RedisClient(object):
    """
    redis client library
    """

    def __init__(self, host, port, db, timeout, auth):
        try:
            self._connection = Telnet(host, port, timeout)
        except socket.error:
            raise base.BlackbirdPluginError(
                'Could not connect {host}:{port}'
                ''.format(host=host, port=port)
            )

        self._timeout = timeout
        if auth != '':
            if self.execute('AUTH', auth) != 'OK':
                raise base.BlackbirdPluginError('Could not AUTH')
        if db:
            if self.execute('SELECT', db) != 'OK':
                raise base.BlackbirdPluginError('Could not select db %d' % db)

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
            raise base.BlackbirdPluginError('Redis error: %s' % rest)
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
            return list([self.read_command() for _ in range(int(rest))])
        else:
            raise base.BlackbirdPluginError(
                'Unknown response prefix "%s"' % prefix
            )

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

        self.__data['key'] = self.key
        self.__data['value'] = self.value
        self.__data['host'] = self.host
        self.__data['clock'] = self.clock


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
            "host = string(default='127.0.0.1')",
            "port = integer(0, 65535, default=6379)",
            "db = integer(0, 15, default=0)",
            "auth = string(default='')",
            "timeout = integer(default=10)",
            "hostname = string(default={0})".format(self.detect_hostname()),
        )
        return self.__spec
