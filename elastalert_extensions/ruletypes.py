# -*- coding: utf-8 -*-
from elastalert.ruletypes import BlacklistRule, CompareRule
from elastalert.util import hashable, lookup_es_key, elastalert_logger


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
