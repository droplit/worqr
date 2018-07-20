# Worqr

A distributed, reliable, job queueing system that uses redis as a backend.

### Terminology

Task - something a worker consumes.
Worker - a distributed agent that consumes tasks.
Queue - a first in, first out list of Tasks.

### Basic Functionality

enqueue - add Task to Queue
`LPUSH ToDoQ1, T1`

startTask - move a Task from a "to do" list to a "doing" list.

```
RPOPLPUSH ToDoQ1, W1Q1Doing
SADD W1Doing, Process1
```

deleteInProcessTask - removes a Task from a Worker's personal "to do" list.

```
DEL Process1
SREM Process1
```

requeueTask - moves a Task from a Worker's list into a top level "to do" list

```
SMEMBERS W1Q1Doing
RPOPLPUSH Process1, ToDoQ1
DEL Process1
SREM W1Q1Doing, Process1
```

registerWorker - register Worker as existing and available for work.

```
SADD RegisteredWorkers, Worker1
SET Worker1, READY
```

stopNewWork - unsubscribe from new Task event queue.

```
UNSUBSCRIBE ToDoQ1
```

stopWorking - finish working on current Tasks. **Does this mean the worker needs to be deregistered?**

```
requeueTask
DEL W1Q1Doing
```

startWorking - subscribe to Task event queue and start work.

```
SUBSCRIBE ToDoQ1
startTask
```

##### Digest Cycle

Keep Alive Worker

```
Every 1 second
	SETEX Worker1, 3
```

Check if Worker is alive

```
SMEMBERS RegisteredWorkers
MGET <list of registered workers>
For each Worker in RegisteredWorkers
	if Worker == NIL
	for each member in WorkerQueueDoing list requeueTask
```

#### Questions

1. What types of queues will there be?
2. Is priority going to be managed by a spread of queues?
3. What types of workers will there be?
4. Can you have a worker that is able to consume different types of queues?
5. Should tasks have a failure counter?
6. Should a task be on X many instances before it's considered available for consumption?
7. What happens if something is completed more than once?
8. Are there any guarantees on message/object delivery?