# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals
from datetime import time
from dateutil import parser
from elastalert import alerts
from kombu import Connection, Exchange
from kombu.pools import producers
from os import environ, path
from pytz import timezone


utc = timezone('UTC')


def parse_time(s):
    if not s:
        return None
    return time(*tuple(map(int, s.split(':'))))


class InTimeframe(object):
    def __init__(self, timestamp_field, schedule):
        self.timestamp_field = timestamp_field
        self.tz = timezone(schedule.get('timezone', 'UTC'))
        self.from_ = parse_time(schedule.get('from', None))
        self.to = parse_time(schedule.get('to', None))

    def __call__(self, entry):
        dt = parser.parse(entry[self.timestamp_field])
        if not dt.tzinfo:
            dt = utc.localize(dt)
        t = dt.astimezone(self.tz).time()
        if self.from_ and t < self.from_:
            return False
        if self.to and t > self.to:
            return False
        return True


class ScheduledAlerter(object):
    def __init__(self, rule):
        self.alerter = next((x for x in self.__class__.__bases__
                             if issubclass(x, alerts.Alerter)),
                            None)
        if self.alerter:
            self.alerter.__init__(self, rule)

        self.in_timeframe = InTimeframe(
            timestamp_field=rule['timestamp_field'],
            schedule=rule.get('schedule', {}))

    def alert(self, matches):
        matches = filter(self.in_timeframe, matches)
        if matches:
            self.alerter.alert(self, matches)

    def get_info(self):
        if self.alerter:
            info = self.alert.get_info(self)
        else:
            info = {'type': ''}
        info['type'] = 'scheduled_{}'.format(info['type'])
        return info


class ScheduledDebugAlerter(ScheduledAlerter, alerts.DebugAlerter):
    pass


class AmqpAlerter(alerts.Alerter):
    """ The amqp alerter publishes alerts via amqp to a broker. """
    def __init__(self, rule):
        super(AmqpAlerter, self).__init__(rule)
        params = {
            'host': self.get_param('amqp_host', 'mq'),
            'port': int(self.get_param('amqp_port', '5672')),
            'vhost': self.get_param('amqp_vhost', '/'),
            'username': self.get_param('amqp_username', 'guest'),
            'password': self.get_param('amqp_password', None),
        }
        if not params['password']:
            with open(path.join('/', 'config', params['username']), 'r') as pwd_file:
                params['password'] = pwd_file.read()
        self._url = (
            'amqp://{username}:{password}@{host}:{port}/{vhost}'
            .format(**params)
        )
        exchange = self.get_param('amqp_exchange', 'alert')
        self._exchange = Exchange(exchange, type='fanout')
        self._routing_key = self.get_param('amqp_routing_key', 'alert')
        self._conn = None

    def get_param(self, name, default):
        environ_name = name.upper()
        return self.rule.get(name, environ.get(environ_name, default))

    def alert(self, matches):
        body = {
            'rule': self.rule['name'],
            'matches': matches,
        }

        with producers[self.conn()].acquire(block=True) as producer:
            for match in matches:
                body = {
                    'rule': self.rule['name'],
                    'match': match,
                }
                producer.publish(body,
                                 serializer='json',
                                 exchange=self._exchange,
                                 routing_key=self._routing_key)

    def conn(self):
        if not self._conn:
            self._conn = Connection(self._url)
        return self._conn

    def get_info(self):
        return {'type': 'amqp'}
