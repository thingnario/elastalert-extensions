from datetime import timedelta

from elastalert_extensions import ruletypes


def test_update_profile(mock_time, mock_getmtime, mock_json_load, mock_ruletypes_open):
    mock_time.return_value = 1514764800 + 3600
    mock_getmtime.return_value = 1514764800
    mock_json_load.return_value = {'device1': 660, 'device2': 1800}

    rule = ruletypes.ProfiledFrequencyRule({
        'num_events': 1,
        'timeframe': 1800,
        'profile': '/etc/profile.json',
    })
    rule._profile_ts = rule._update_ts = 1514764800 - 60

    assert rule.profile == {'device1': timedelta(seconds=660),
                            'device2': timedelta(seconds=1800)}
    mock_getmtime.assert_called_with('/etc/profile.json')
    mock_ruletypes_open.assert_called_with('/etc/profile.json', 'r')


def test_before_update_interval(mock_time, mock_getmtime, mock_json_load, mock_ruletypes_open):
    mock_time.return_value = 1514764800 + 10
    mock_getmtime.return_value = 1514764800
    mock_json_load.return_value = {'device1': 660, 'device2': 1800}

    rule = ruletypes.ProfiledFrequencyRule({
        'num_events': 1,
        'timeframe': 1800,
        'profile': '/etc/profile.json',
    })
    rule._profile_ts = 1514764800 - 60
    rule._update_ts = 1514764800

    rule.profile
    mock_getmtime.assert_not_called()
    mock_ruletypes_open.assert_not_called()


def test_profile_mtime_in_the_past(mock_time, mock_getmtime, mock_json_load, mock_ruletypes_open):
    mock_time.return_value = 1514764800 + 3600
    mock_getmtime.return_value = 1514764800 - 60
    mock_json_load.return_value = {'device1': 660, 'device2': 1800}

    rule = ruletypes.ProfiledFrequencyRule({
        'num_events': 1,
        'timeframe': 1800,
        'profile': '/etc/profile.json',
    })
    rule._profile_ts = 1514764800 - 60
    rule._update_ts = 1514764800

    rule.profile
    mock_getmtime.assert_called_with('/etc/profile.json')
    mock_ruletypes_open.assert_not_called()


def test_last_update_time_in_the_future(mock_time, mock_getmtime, mock_json_load, mock_ruletypes_open):
    mock_time.return_value = 1514764800 + 3600
    mock_getmtime.return_value = 1514764800 - 60
    mock_json_load.return_value = {'device1': 660, 'device2': 1800}

    rule = ruletypes.ProfiledFrequencyRule({
        'num_events': 1,
        'timeframe': 1800,
        'profile': '/etc/profile.json',
    })
    rule._profile_ts = 1514764800 - 60
    rule._update_ts = 1514764800 + 86400

    rule.profile
    mock_getmtime.assert_called_with('/etc/profile.json')
    mock_ruletypes_open.assert_not_called()


def test_last_profile_mtime_in_the_future(mock_time, mock_getmtime, mock_json_load, mock_ruletypes_open):
    mock_time.return_value = 1514764800 + 60
    mock_getmtime.return_value = 1514764800 + 3600
    mock_json_load.return_value = {'device1': 660, 'device2': 1800}
    rule = ruletypes.ProfiledFrequencyRule({
        'num_events': 1,
        'timeframe': 1800,
        'profile': '/etc/profile.json',
    })
    rule._profile_ts = 1514764800 + 86400
    rule._update_ts = 1514764800
    rule.profile
    mock_getmtime.assert_called_with('/etc/profile.json')
    mock_ruletypes_open.assert_called_with('/etc/profile.json', 'r')


def test_force_reload_everyday(mock_time, mock_getmtime, mock_json_load, mock_ruletypes_open):
    mock_time.return_value = 1514764800.0 + 86400.0 + 60.0
    mock_getmtime.return_value = 1514764800.0
    mock_json_load.return_value = {'device1': 660, 'device2': 1800}

    rule = ruletypes.ProfiledFrequencyRule({
        'num_events': 1,
        'timeframe': 1800,
        'profile': '/etc/profile.json',
    })
    rule._profile_ts = 1514764800.0
    rule._update_ts = 1514764800.0 + 86400.0

    rule.profile
    mock_getmtime.assert_called_with('/etc/profile.json')
    mock_ruletypes_open.assert_called_with('/etc/profile.json', 'r')
