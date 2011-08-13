import random

import eventlet
import task

@task.ify()
def long_action(number, task_id, progress):
    tries = (progress or 0) + 1
    eventlet.sleep(random.randint(1, 10) / 10)
    if random.randint(1, 10) > 3:
        print "Action %s Failed" % number
        raise task.Failure(tries)
    print "Action %s Succeeded after %s tries" % (number, tries)
    return tries

task.setup_db('sqlite://') # in memory db

task_ids = []
for i in xrange(10):
    task_ids.append(long_action(i))

while not all(task.is_complete(task_id) for task_id in task_ids):
    task_id =  task.claim()
    if task_id:
        eventlet.spawn_n(task.run, task_id)
    eventlet.sleep(0)

for task_id in task_ids:
    print task.get(task_id)['attempts']

