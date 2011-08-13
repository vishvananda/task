task.py
=======

Happy python library for keeping track of tasks and rerunning them.  Creating and running a task is easy:

  import task

  @task.ify()
  def doit(**kwargs):
     print kwargs['task_id']

  task.setup_db('sqlite://') # in memory db
  task_id = doit()
  task.run(task_id)

The wrapped method needs to accept **kwargs. It will be passed two kwargs: task_id and progress.  Task_id holds the identifier for the task should you need it.  Progress holds the last data you returned from the method.

The sexy way to use tasks is to define a generator that yields for each phase of the task.  See the tests and example_* for examples of running tasks.  If you have nose installed you can run the tests via nosetests.
