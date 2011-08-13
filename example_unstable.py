import random
import task

@task.ify()
def unstable_action(task_id, progress):
    progress = progress or {'phase': 0}
    if progress['phase'] < 1:
        progress['phase'] += 1
        yield progress
    if progress['phase'] < 2:
        if random.randint(1, 10) > 5:
            raise task.Failure(progress)
        progress['phase'] += 1
        yield progress
    if progress['phase'] < 3:
        if random.randint(1, 10) > 1:
            raise task.Failure(progress)
        progress['phase'] += 1
        yield progress

task.setup_db('sqlite://') # in memory db
task_id = unstable_action()

def get_results(results):
    try:
        for progress in results:
            print "Phase %s Complete" % progress['phase']
    except task.Failure as ex:
        print "Phase %s Failed" % (ex.progress['phase'] + 1)

while not task.is_complete(task_id):
    results = task.run(task_id)
    get_results(results)

print "Task Completed"
