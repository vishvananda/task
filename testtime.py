# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 Vishvananda Ishaya
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""Time testing helper functions."""


import datetime


class DateTimeOverrider(object):
    def __init__(self, override_time):
        self.dt = datetime.datetime
        self.override_time = override_time

    def __getattr__(self, key):
        return getattr(self.dt, key)

    def utcnow(self):
        """Overridden utcnow."""
        return self.override_time


def set_time_override(override_time=datetime.datetime.utcnow()):
    """Override datetime.datetime.utcnow to return a constant time."""
    if not isinstance(datetime.datetime, DateTimeOverrider):
        datetime.datetime = DateTimeOverrider(override_time)


def advance_time_delta(timedelta):
    """Advance overriden time using a datetime.timedelta."""
    datetime.datetime.override_time += timedelta


def advance_time_seconds(seconds):
    """Advance overriden time by seconds."""
    advance_time_delta(datetime.timedelta(0, seconds))


def clear_time_override():
    """Remove the overridden time."""
    if isinstance(datetime.datetime, DateTimeOverrider):
        datetime.datetime = datetime.datetime.dt

