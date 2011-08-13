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

import datetime
import unittest

import mock_datetime
import task

@task.ify('another_name')
def one_name(task_id, progress):
    """Finish the task and return the id."""
    return task_id

@task.ify()
def finish(task_id, progress):
    """Finish the task and return the id."""
    return task_id

@task.ify()
def retry(*args, **kwargs):
    """Complete the task if it is re-run."""
    task_id = kwargs.pop('task_id')
    progress = kwargs.pop('progress')
    if progress is None:
        raise task.Failure('fail')
    return (args, kwargs)

@task.ify(auto_update=False)
def manual_retry(*args, **kwargs):
    """Complete the task if it is re-run."""
    task_id = kwargs.pop('task_id')
    progress = kwargs.pop('progress')
    if progress is None:
        task.fail(task_id, 'fail')
        return 'fail'
    task.finish(task_id)
    return (args, kwargs)

@task.ify()
def generator_retry(*args, **kwargs):
    """Complete the task if it is re-run."""
    task_id = kwargs.pop('task_id')
    progress = kwargs.pop('progress')
    if progress is None:
        raise task.Failure('fail')
    yield (args, kwargs)
    # NOTE(vish): the second call will stop the iteration and finish

@task.ify(auto_update=False)
def nofail(*args, **kwargs):
    """Complete the task if it is re-run without failing."""
    task_id = kwargs.pop('task_id')
    progress = kwargs.pop('progress')
    if progress is None:
        task.update(task_id, 'one')
        return 'one'
    task.finish(task_id)
    return (args, kwargs)

@task.ify()
def complex_task(number, *args, **kwargs):
    task_id = kwargs.pop('task_id')
    progress = kwargs.pop('progress')
    # NOTE(vish): tasks have to be smart enough to give
    #             the same results if restarted
    start = 0
    if progress and isinstance(progress, int):
        start = progress + 1
    for x in xrange(start, number):
        yield x

class ObjectWithTasks(object):
    def __init__(self, value):
        super(ObjectWithTasks, self).__init__()
        self.value = value

    @task.ify()
    def retry_value(self, *args, **kwargs):
        """Complete the task if it is re-run."""
        task_id = kwargs.pop('task_id')
        progress = kwargs.pop('progress')
        if progress is None:
            raise task.Failure('fail')
        return self.value


class TaskTestCase(unittest.TestCase):
    """Test nova.task functionality"""

    def setUp(self):
        task.setup_db('sqlite://')
        task.inject_now_method(mock_datetime.utcnow)
        super(TaskTestCase, self).setUp()

    def test_finish_task(self):
        task_id = finish()
        task.run(task_id)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))

    def test_change_task_name(self):
        task_id = one_name()
        task.run(task_id)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))
        self.assertEqual(task.get(task_id)['task_name'], 'another_name')

    def test_retry_task(self):
        task_id  = retry()
        result = task.run(task_id)
        self.assertEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        result = task.run(task_id)
        self.assertNotEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))

    def test_manual_retry_task(self):
        task_id = manual_retry()
        result = task.run(task_id)
        self.assertEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        result = task.run(task_id)
        self.assertNotEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))

    def test_generator_retry_task(self):
        task_id = generator_retry()
        rval = task.run(task_id)
        result = rval.next()
        self.assertEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        rval = task.run(task_id)
        result = rval.next()
        self.assertNotEqual('fail', result)
        self.assertRaises(StopIteration, rval.next)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))

    def test_complex_task(self):
        task_id = complex_task(10)
        rval = task.run(task_id)
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        total = 0
        for x in xrange(5):
            total += rval.next()
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        rval = task.run(task_id)
        for x in xrange(5):
            total += rval.next()
        self.assertRaises(StopIteration, rval.next)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))
        task_id = complex_task(10)
        rval = task.run(task_id)
        self.assertEqual(total, sum(list(rval)))

    def test_object_retry(self):
        obj = ObjectWithTasks(42)
        task_id = obj.retry_value()
        result = task.run(task_id)
        self.assertEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        result = task.run(task_id)
        self.assertNotEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))
        self.assertEqual(result, 42)

    def test_object_retry_deleted_object(self):
        obj = ObjectWithTasks(42)
        task_id = obj.retry_value()
        task.run(task_id)
        self.assertTrue(task.exists(task_id))
        self.assertFalse(task.is_complete(task_id))
        del obj
        result = task.run(task_id)
        self.assertNotEqual('fail', result)
        self.assertTrue(task.exists(task_id))
        self.assertTrue(task.is_complete(task_id))
        self.assertEqual(result, 42)

    def test_retry_same_args(self):
        args = (75, 'arbitrary', {'more': 7.5})
        task_id = retry(*args)
        task.run(task_id)
        ret, _ = task.run(task_id)
        self.assertEqual(args, ret)

    def test_retry_same_kwargs(self):
        kwargs = {'num': 75, 'str': 'arbitrary', 'dict': {'more': 7.5}}
        task_id = retry(**kwargs)
        task.run(task_id)
        _, ret = task.run(task_id)
        self.assertEqual(kwargs, ret)

    def test_rerun_old_tasks(self):
        mock_datetime.set_time_override()
        try:
            task_id1 = nofail()
            task.run(task_id1)
            task_id2 = nofail()
            task.run(task_id2)
            mock_datetime.advance_time_seconds(60)
            task_id3 = nofail()
            task.run(task_id3)
            self.assertFalse(task.is_complete(task_id1))
            task.run(task_id1)
            import logging
            logging.warn(task.get(task_id1)['updated_at'])
            logging.warn(task.get(task_id2)['updated_at'])
            logging.warn(task.get(task_id3)['updated_at'])
            timeout = mock_datetime.utcnow() - datetime.timedelta(seconds=30)
            num = task.timeout(timeout)
            self.assertEqual(num, 1)
            task_id = task.claim()
            self.assertEqual(task_id, task_id2)
            self.assertEqual(task.claim(), None)
            task.run(task_id)
            self.assertTrue(task.is_complete(task_id2))
            self.assertFalse(task.is_complete(task_id3))
        finally:
            mock_datetime.clear_time_override()
