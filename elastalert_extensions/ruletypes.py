# -*- coding: utf-8 -*-
import copy
from datetime import timedelta
import json
import os.path
import time

from elastalert.util import dt_to_ts
from elastalert.util import EAException
from elastalert.util import elastalert_logger
from elastalert.util import hashable
from elastalert.util import lookup_es_key
from elastalert.util import new_get_event_ts
from elastalert.util import pretty_ts
from elastalert.util import ts_to_dt
from elastalert.ruletypes import BlacklistRule, CompareRule
from elastalert.ruletypes import EventWindow
from elastalert.ruletypes import FrequencyRule


UPDATE_INTERVAL = 60.0
FORCE_UPDATE_INTERVAL = 86400.0


class BlacklistDurationRule(CompareRule):
    required_options = frozenset(['query_key', 'compound_compare_key',
                                  'ignore_null', 'blacklist', 'timeframe'])
    change_map = {}
    occurrence_time = {}

    def __init__(self, rules, args=None):
        super(BlacklistDurationRule, self).__init__(rules, args=None)
        self.expand_entries('blacklist')

    def compare(self, event):
        key = hashable(lookup_es_key(event, self.rules['query_key']))
        values = []
        elastalert_logger.debug(" Previous Values of compare keys  " + str(self.occurrences))
        for val in self.rules['compound_compare_key']:
            lookup_value = lookup_es_key(event, val)
            values.append(lookup_value)
        elastalert_logger.debug(" Current Values of compare keys   " + str(values))

        start = changed = False
        for val in values:
            if not isinstance(val, bool) and not val and self.rules['ignore_null']:
                return False
        # If we have seen this key before, compare it to the new value
        if key in self.occurrences:
            for idx, previous_values in enumerate(self.occurrences[key]):
                elastalert_logger.debug(" " + str(previous_values) + " " + str(values[idx]))
                start = (previous_values != values[idx] and
                         values[idx] in self.rules['blacklist'])
                changed = (previous_values != values[idx] and
                           previous_values in self.rules['blacklist'])
                if start or changed:
                    break
            if changed:
                entry = [self.occurrences[key], None, None]
                # If using timeframe, only return true if the time delta is < timeframe
                if key in self.occurrence_time:
                    changed = (event[self.rules['timestamp_field']] - self.occurrence_time[key] <=
                               self.rules['timeframe'])
                    old_time = self.occurrence_time[key]
                    new_time = event[self.rules['timestamp_field']]
                    duration = new_time - old_time
                    entry[1], entry[2] = [old_time, duration.total_seconds()]
                self.change_map[key] = tuple(entry)

        if key not in self.occurrences or start or changed:
            # Update the current value and time
            elastalert_logger.debug(" Setting current value of compare keys values " + str(values))
            self.occurrences[key] = values
            self.occurrence_time[key] = event[self.rules['timestamp_field']]
        elastalert_logger.debug("Final result of comparision between previous and current values " +
                                str(changed))
        return changed

    def add_match(self, match):
        # TODO this is not technically correct
        # if the term changes multiple times before an alert is sent
        # this data will be overwritten with the most recent change
        change = self.change_map.get(hashable(lookup_es_key(match, self.rules['query_key'])))
        extra = {}
        if change:
            extra = {'value': change[0],
                     'start_time': change[1],
                     'duration': change[2]}
            elastalert_logger.debug("Description of the changed records  " +
                                    str(dict(match.items() + extra.items())))
        super(BlacklistDurationRule, self).add_match(dict(match.items() + extra.items()))


class CompoundBlacklistRule(BlacklistRule):
    """ A CompareRule where the compare function checks a given key against a blacklist """
    def compare(self, event):
        terms = lookup_es_key(event, self.rules['compare_key'])
        if not isinstance(terms, list):
            terms = [terms]
        for term in terms:
            if term in self.rules['blacklist']:
                return True
        return False


class ProfiledFrequencyRule(FrequencyRule):
    """ A rule that matches if num_events number of events occur within a timeframe """
    required_options = frozenset(['num_events', 'timeframe', 'profile'])

    def __init__(self, *args):
        super(ProfiledFrequencyRule, self).__init__(*args)
        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        self.get_ts = new_get_event_ts(self.ts_field)
        self.attach_related = self.rules.get('attach_related', False)
        self._profile = {}
        self._profile_ts = 0.0
        self._update_ts = 0.0

    def timeframe(self, key):
        return self.profile.get(key, self.rules['timeframe'])

    @property
    def profile(self):
        profile_path = self.rules['profile']
        now = time.time()

        try:
            if not (self._update_ts <= now < self._update_ts + UPDATE_INTERVAL):
                # Check if updated
                ts = os.path.getmtime(profile_path)
                self._update_ts = now
            else:
                # Skip
                ts = self._profile_ts

            if ts > self._profile_ts or ts > now or now > self._profile_ts + FORCE_UPDATE_INTERVAL:
                elastalert_logger.info('Reloading profile %s', profile_path)
                with open(profile_path, 'r') as profile_file:
                    profile = json.load(profile_file)
                    self._profile = {k: timedelta(seconds=profile[k])
                                     for k in profile}
                    self._profile_ts = now
        except (OSError, IOError, ValueError) as e:
            elastalert_logger.error('Cannot load profile %s: %s', profile_path, e)
        return self._profile

    def add_count_data(self, data):
        """ Add count data to the rule. Data should be of the form {ts: count}. """
        if len(data) > 1:
            raise EAException('add_count_data can only accept one count at a time')

        (ts, count), = data.items()

        event = ({self.ts_field: ts}, count)
        self.occurrences.setdefault(
            'all',
            EventWindow(self.timeframe('all'), getTimestamp=self.get_ts)
        ).append(event)
        self.check_for_match('all')

    def add_terms_data(self, terms):
        for timestamp, buckets in terms.iteritems():
            for bucket in buckets:
                event = ({self.ts_field: timestamp,
                          self.rules['query_key']: bucket['key']}, bucket['doc_count'])
                self.occurrences.setdefault(
                    bucket['key'],
                    EventWindow(self.timeframe(bucket['key']), getTimestamp=self.get_ts)
                ).append(event)
                self.check_for_match(bucket['key'])

    def add_data(self, data):
        if 'query_key' in self.rules:
            qk = self.rules['query_key']
        else:
            qk = None

        for event in data:
            if qk:
                key = hashable(lookup_es_key(event, qk))
            else:
                # If no query_key, we use the key 'all' for all events
                key = 'all'

            # Store the timestamps of recent occurrences, per key
            self.occurrences.setdefault(
                key, EventWindow(self.timeframe(key), getTimestamp=self.get_ts)).append((event, 1))
            self.check_for_match(key, end=False)

        # We call this multiple times with the 'end' parameter because subclasses
        # may or may not want to check while only partial data has been added
        if key in self.occurrences:  # could have been emptied by previous check
            self.check_for_match(key, end=True)

    def garbage_collect(self, timestamp):
        """ Remove all occurrence data that is beyond the timeframe away """
        stale_keys = []
        for key, window in self.occurrences.iteritems():
            if timestamp - lookup_es_key(window.data[-1][0], self.ts_field) > self.timeframe(key):
                stale_keys.append(key)
        map(self.occurrences.pop, stale_keys)

    def get_match_str(self, match):
        lt = self.rules.get('use_local_time')
        match_ts = lookup_es_key(match, self.ts_field)
        key = match.get('key', 'all')
        starttime = pretty_ts(dt_to_ts(ts_to_dt(match_ts) - self.timeframe(key)), lt)
        endtime = pretty_ts(match_ts, lt)
        message = 'At least %d events occurred between %s and %s\n\n' % (self.rules['num_events'],
                                                                         starttime,
                                                                         endtime)
        message = json.dumps(match)
        return message


class ProfiledThresholdRule(ProfiledFrequencyRule):
    """ A rule that matches when there is a low number of events given a timeframe. """
    required_options = frozenset(['threshold', 'timeframe', 'profile'])

    def __init__(self, *args):
        super(ProfiledThresholdRule, self).__init__(*args)
        self.threshold = self.rules['threshold']
        self.above = self.rules.get('above_name', 'above')
        self.below = self.rules.get('below_name', 'below')

        # Dictionary mapping query keys to the first events
        self.first_event = {}
        self._last_status = {}

    def check_for_match(self, key, end=True):
        # This function gets called between every added document with end=True after the last
        # We ignore the calls before the end because it may trigger false positives
        if not end:
            return

        most_recent_ts = self.get_ts(self.occurrences[key].data[-1])
        if self.first_event.get(key) is None:
            self.first_event[key] = most_recent_ts

        # Don't check for matches until timeframe has elapsed
        if most_recent_ts - self.first_event[key] < self.timeframe(key):
            return

        # Match if, after removing old events, we hit num_events
        count = self.occurrences[key].count()
        status = self.below if count < self.rules['threshold'] else self.above

        if status != self._last_status.get(key, None):
            # Do a deep-copy, otherwise we lose the datetime type
            # in the timestamp field of the last event
            event = copy.deepcopy(self.occurrences[key].data[-1][0])
            event.update(key=key, count=count, status=status)
            self.add_match(event)

            # After adding this match, leave the occurrences windows alone since it will
            # be pruned in the next add_data or garbage_collect, but reset the first_event
            # so that alerts continue to fire until the threshold is passed again.
            least_recent_ts = self.get_ts(self.occurrences[key].data[0])
            timeframe_ago = most_recent_ts - self.timeframe(key)
            self.first_event[key] = min(least_recent_ts, timeframe_ago)

        self._last_status[key] = status

    def get_match_str(self, match):
        ts = match[self.rules['timestamp_field']]
        lt = self.rules.get('use_local_time')
        key = match.get('key', 'all')
        message = 'An abnormally low number of events occurred around %s.\n' % (pretty_ts(ts, lt))
        message += 'Between %s and %s, there were less than %s events.\n\n' % (
            pretty_ts(dt_to_ts(ts_to_dt(ts) - self.timeframe(key)), lt),
            pretty_ts(ts, lt),
            self.rules['threshold']
        )
        message = json.dumps(match)
        return message

    def garbage_collect(self, ts):
        # We add an event with a count of zero to the EventWindow for each key.
        # This will cause the EventWindow to remove events that occurred
        # more than one `timeframe` ago, and call onRemoved on them.
        default = ['all'] if 'query_key' not in self.rules else []
        for key in self.occurrences.keys() or default:
            self.occurrences.setdefault(
                key,
                EventWindow(self.timeframe(key), getTimestamp=self.get_ts)
            ).append(
                ({self.ts_field: ts}, 0)
            )
            self.first_event.setdefault(key, ts)
            self.check_for_match(key)
