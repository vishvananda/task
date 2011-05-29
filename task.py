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
"""
Stores tasks and allows them to be run again later.
"""


import datetime
import functools
import inspect
import logging
import pickle
import types
import uuid


_TASKS = {}
_FREE_TASK_IDS = []
_TASK_IDS_BY_NAME = {}
_FREE_TASK_IDS_BY_NAME = {}


def _create(task_name, method, member, *args, **kwargs):
    now = datetime.datetime.utcnow()
    task_id = uuid.uuid4()
    task = {'id': task_id,
            'task_name': task_name,
            'method': method,
            'member': member,
            'args': args,
            'kwargs': kwargs,
            'created_at': now,
            'updated_at': now,
            'active': True,
            'progress': None}
    _TASKS[task_id] = task
    if task_name not in _TASK_IDS_BY_NAME:
        _TASK_IDS_BY_NAME[task_name] = []
    _TASK_IDS_BY_NAME[task_name].append(task_id)
    return task_id


def _is_member(func, args):
    """Checks args to determine if func is a bound method."""
    if not args:
        return False
    ismethod = False
    for item in inspect.getmro(type(args[0])):
        for x in inspect.getmembers(item):
            if 'im_func' in dir(x[1]):
                ismethod = x[1].im_func == func
                if ismethod:
                    break
        else:
            continue
        break
    return ismethod


def ify(name=None, auto_update=True):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            if 'task_id' in kwargs:
                task_id = kwargs.pop('task_id')
                progress = kwargs.pop('progress')
            else:
                task_name = name or func.__name__
                if _is_member(wrapped, args):
                    method = func.__name__
                    member = True
                else:
                    method = wrapped
                    member = False
                task_id = _create(task_name, method, member, *args, **kwargs)
                progress = None
                logging.debug('Starting task %s', task_id)
            rv = func(task_id=task_id, progress=progress, *args, **kwargs)
            if auto_update:
                if isinstance(rv, types.GeneratorType):
                    def gen():
                        for orig_rv in rv:
                            update(task_id, orig_rv)
                            yield orig_rv
                        finish(task_id)
                    return gen()
                update(task_id, rv)
            return rv
        return wrapped
    return wrapper


def get(task_id):
    """Get task from id."""
    return _TASKS.get(task_id)


def claim(task_name=None):
    """Get a free task_id if available optionally by task_name."""
    try:
        if task_name:
            task_id = _FREE_TASK_IDS_BY_NAME.get(task_name, []).pop()
            _FREE_TASK_IDS.remove(task_id)
        else:
            task_id = _FREE_TASK_IDS.pop()
            task_name = _TASKS[task_id]['task_name']
            _FREE_TASK_IDS_BY_NAME[task_name].remove(task_id)
        return task_id
    except IndexError:
        return None


def timeout(time, task_name=None):
    """Free tasks by time and optional task_name.

    :returns: number of tasks freed"""
    num_freed = 0
    if task_name:
        items = [_TASKS[task_id] for task_id
                 in _TASK_IDS_BY_NAME.get(task_name, [])]
    else:
        items = _TASKS.itervalues()
    for task in items:
        if task['updated_at'] < time and task['active']:
            task['active'] = False
            task['updated_at'] = datetime.datetime.utcnow()
            _FREE_TASK_IDS.append(task['id'])
            if task_name not in _FREE_TASK_IDS_BY_NAME:
                _FREE_TASK_IDS_BY_NAME[task['task_name']] = []
            _FREE_TASK_IDS_BY_NAME[task['task_name']].append(task['id'])
            num_freed += 1
    return num_freed


def run(task_id):
    """Runs the task with task id.

    Underlying method will receive two kwargs:
        task_id = id of the current task for updating
        progress = last progress passed to task_update"""
    task = _TASKS[task_id]
    if task['member']:
        method = getattr(task['args'][0], task['method'])
    else:
        method = task['method']
    _TASKS[task_id]['updated_at'] = datetime.datetime.utcnow()
    return method(task_id=task['id'], progress=task['progress'],
                  *task['args'], **task['kwargs'])


def update(task_id, progress):
    """Update the current task progress."""
    _TASKS[task_id]['updated_at'] = datetime.datetime.utcnow()
    _TASKS[task_id]['progress'] = progress


def finish(task_id):
    """Mark the task completed."""
    _TASKS[task_id]['updated_at'] = datetime.datetime.utcnow()
    _TASKS[task_id]['completed_at'] = datetime.datetime.utcnow()
    _TASKS[task_id]['active'] = False
    logging.debug('Finished task %s', task_id)


def is_active(task_id):
    """True if the task is active."""
    try:
        return _TASKS[task_id]['active']
    except KeyError:
        return False


def is_complete(task_id):
    """Completed if the task is done."""
    try:
        return _TASKS[task_id]['completed_at'] is not None
    except KeyError:
        return False


def exists(task_id):
    """True if the task exists."""
    return task_id in _TASKS


def dump():
    """Dump task data."""
    data = {'_TASKS': _TASKS,
            '_FREE_TASK_IDS': _FREE_TASK_IDS,
            '_TASK_IDS_BY_NAME': _TASK_IDS_BY_NAME,
            '_FREE_TASK_IDS_BY_NAME': _FREE_TASK_IDS_BY_NAME}
    with open('task.data', 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)


def load():
    """Load task data."""
    with open('task.data', 'rb') as f:
        data = pickle.load(f)
    _TASKS = data['_TASKS']
    _FREE_TASK_IDS = data['_FREE_TASK_IDS']
    _TASK_IDS_BY_NAME = data['_TASK_IDS_BY_NAME']
    _FREE_TASK_IDS_BY_NAME = data['_FREE_TASK_IDS_BY_NAME']
