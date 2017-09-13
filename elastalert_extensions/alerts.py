# -*- coding: utf-8 -*-
from datetime import time
from dateutil import parser
from elastalert import alerts
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
