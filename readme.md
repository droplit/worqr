<p align="center">
  <a href="https://github.com/droplit/worqr">
    <img height="400" width="400" src="https://raw.githubusercontent.com/droplit/content/master/worqr-800x800.png">
  </a>
  <p align="center" style="margin-top:-70px;font-weight:bold;font-size:large">Atomic Redis Queue</p>
</p>

<a href="https://droplit.io">
    <img height="70" width="280" src="https://raw.githubusercontent.com/droplit/content/master/createdByDroplitBanner-worqr100x400.png" target="_blank">
</a>

[![NPM](https://nodei.co/npm/worqr.png)](https://www.npmjs.com/package/worqr)

![node](https://img.shields.io/github/license/droplit/worqr.svg?style=flat-square)

# Worqr

A distributed, reliable, atomic, work queueing system that only requires redis as the backend.

Attributes:

- Ensure a task is performed **exactly once**.
- Only Redis is required as a back-end server. No server components to install. Operates purely redis-based, peer-supervised in your application.
- Every transaction is fully atomic (queue never left in inconsistent state).
- When workers fail, all tasks are put back into the queue at the front.
- Queue doesn't keep any item history.
- As a work queue, each item is only delivered to exactly one worker.
- Work items are not enveloped, so there is no encoding/decoding overhead or chance for the format to change in future releases.
- Uses pub/sub for instantaneous new work notifications(no spin locks or blocking calls).
- When a worker fails, all in-process work is automatically rescheduled to other workers.
- Worqr is written in TypeScript, so you get automatic typings included.

Is **Worqr** for me?

- You need to distribute a task workload
- You are already using Redis in your application
- You don't want to run a queue service or pay per-transaction fees

If the answer to all three of those questions is yes, then `worqr` is for you.

## Theory of Operation

The primary unit of work to be done is a **Task**. Tasks are organized into **Queues**. Queues are serviced by **Workers**. When a worker takes a task, the task is assigned to that worker's in-process list. If a worker fails to keep it's active flag from expiring, all its in-process tasks are returned to the work queue.

Worqr can handle one-time tasks as well as persistent tasks. A persistent task is a task that never completes. For example, if your worker needs to keep a set of open connections to other applications, each of those connections can be managed with a task so they are  opened exactly once by exactly one worker.

## Terminology

**Task** - something a worker consumes.

**Process** - a task that is being worked on.

**Worker** - a distributed agent that consumes tasks.

**Queue** - a first in, first out list of Tasks.

## Getting started

### Initializing Worqr

```typescript
let worqr = new Worqr({ host: <domain/ip>, port: <port #>, password: <pwd>}, {redisKeyPrefix: <unique namespace> });
```
> NOTE: Worqr will open two connections to Redis. One for data and one for subscriptions.

You can also supply an existing data and/or subscription connection to prevent Worqr from opening it's own.

```typescript
let worqr = new Worqr({ data: <RedisClient instance>, subscribe: <RedisClient instance> });
```
> NOTE: These must be separate Redis connections if supplying both.

### Queing work

```typescript
worqr.enqueue('<queue name>', '<task>');
```
> NOTE: Tasks are strings. Your code is responsible for serializing any complex objects.

### Stopping work

```typescript
worqr.cancelTasks('<queue name>', '<task>');
```
> NOTE: Worqr will cancel all instances of the specified task if more than one exists. Task cancellation must be supported by the worker process as well if the task is already in-process.

### Getting setup to do work
If this is a worker process (a process that will perform work), you must start the worker before you can accept work from a queue. Do not do this in a process that only queues tasks.

```typescript
worqr.startWorker();
```

`startWorker` method returns a promise, so it also supports `await`:

```typescript
await worqr.startWorker();
```
> NOTE: Once a worker has started, it will periodically refresh a redis key. Make sure your [event loop](https://nodejs.org/en/docs/guides/event-loop-timers-and-nexttick/) doesn't get CPU starved or your worker will be failed and its work will be recycled back into the queue. If your event loop runs too long, you can use `setImmediate()` to break your long running process into smaller sections.

### Taking work from a queue

```typescript
// Add event handler for queue name first, so we don't miss any work events
worqr.on('<queue name>', (type: string, message: string) => {
    switch (type) {
        case 'work':
            // do work
            // message: the number of new tasks
            doWork();
            ...
            break;
        case 'cancel':
            // stop work
            // message: task to cancel 
            ...
            break;
        case 'delete':
            // work queue was deleted, no action necessarily needed
            // message: empty string
            ...
            break;
        }       
});

// Ask for work from the specified queue
await worqr.startWork('<queue name>');
```

#### Work processing function
Once a task is started, it's called a "process". The process has a `processId` that must be used to uniquely finish or fail that task instance. This is because a task payload is not necessarily unique.

```typescript
async function doWork() {
    const process = await worqr.dequeue('<queue name>');
    if (process) {
        // perform the task `process.task`
        ...
        // complete the task
        await worqr.finishProcess(process.id);
        // setImmediate gives the event loop a chance to perform other tasks first
        setImmediate(() => {
            doWork();
        });
    }
}
```

### Stop working on a task/Fail a task
If something goes wrong while processing a task, you can fail the task to send it back to the queue.

```typescript
await worqr.stopProcess('<processId>');
```

> NOTE: Worqr does not track the historical state of a task. If it fails for some reason that will always fail like invalid input, the task should be marked as finished and that error should be handled in your code.

### Fall all tasks
If your process is shutting down and you want to return all work to the queue, you can fail your worker

```typescript
worqr.failWorker();
```

## Additional command reference

### Get all queue names
```typescript
function getQueues(): Promise<string[]>
```

### Get all current workers
```typescript
public getWorkers(): Promise<string[]>
```

### Get queues that are being worked on by a particular worker
Omitting the `workerId` will use the current workerId
```typescript
private getWorkingQueues(workerId?: string): Promise<string[]>
```

## What's actually happening?

### Enqueue

Add a task to a queue.

In the diagram, the queue name is `Q1` and the task payload is `T1`.

```
enqueue (T1, Q1)

        Q1 : List
        +----------------------+
        | +----+ +----+        |
(1)     | |   *| |    |        |
T1 +--> | |T1  | |T0  |        |
        | +----+ +----+        |
        +----------------------+
         head              tail
 1) LPUSH Q1 T1
 2) PUBLISH Q1 1
 ```

### Start Task

Move a task from a "to do" list to a "doing" list.

In the diagram, the worker is `W1` and the process is `P1`.

```
startTask (Q1, W1, P1)

        Q1 : List                         P1 : List           W1_Q1 : Set
        +----------------------+          +--------+          +---------------------+
        | +----+ +----+        |          | +----+ |          | +----+ +----+       |
        | |    | |   X|        | (1)      | |   *| |  (2)     | |   *| |    |       |
        | |T1  | |T0  |        | -->T0--> | |T0  | |  P1 +--> | |P1  | |P0  |       |
        | +----+ +----+        |          | +----+ |          | +----+ +----+       |
        +----------------------+          +--------+          +---------------------+
         head              tail
 1) RPOPLPUSH Q1 P1
 2) SADD W1_Q1 P1
 ```

### Stop Task

Removes a task from a worker's personal "to do" list.

```
stopTask (P1)

        Q1 : List                         P1 : List           W1_Q1 : Set
        +----------------------+          +--------+          +---------------------+
        | +----+ +----+        |          | +----+ |          | +----+ +----+       |
        | |    | |   *|        | (1)      | |   X| |      (2) | |   X| |    |       |
        | |T1  | |T0  |        | <--T0<-- | |T0  | |          | |P1  | |P0  |       |
        | +----+ +----+        |          | +----+ |          | +----+ +----+       |
        +----------------------+          +--------+          +---------------------+
         head              tail
 1) RPOPLPUSH P1 Q1
 2) SREM W1_Q1 P1
```

### Finish Task

Moves a task from a worker's list into a top level "to do" list.

```
finishTask (P1)

                                          P1 : List           W1_Q1 : Set
                                          +--------+          +---------------------+
                                          | +----+X|          | +----+ +----+       |
                                          | |    | |          | |   X| |    |       |
                                      (1) | |T0  | |      (2) | |P1  | |P0  |       |
                                          | +----+ |          | +----+ +----+       |
                                          +--------+          +---------------------+

 1) DEL P1
 2) SREM W1_Q1 P1
```

### Cancel Tasks

Removes a task from a queue.

Workers listen on `Q1` and "finish" all tasks that are cancelled.

```
cancelTasks (Q1, T1)

        Q1 : List
        +----------------------+
        | +----+ +----+        |
        | |   X| |    |        |
        | |T1  | |T0  |        |
        | +----+ +----+        |
        +----------------------+
         head              tail
 1) LREM Q1 T1 0
 2) PUBLISH Q1_cancel T1
```

### Start Worker

Register a worker as existing and available for work.

```
startWorker (W1)

        Workers : Set
        +----------------------+          2) W1 = "RUN"
        | +----+ +----+        |             W1.TTL = 3 sec
        | |    | |   *|        |     (1)
        | |W0  | |W1  |        | <--+ W1
        | +----+ +----+        |
        +----------------------+

 1) SADD Workers W1
 2) SET W1 RUN EX 3
```

### Keep Worker Alive

Digest cycle.

```
keepWorkerAlive (W1)

Every 1 second
        SET W1 RUN EX 3 XX  (XX = set if exists)

    If SET fails, worker has failed to stay alive and must restart
```

### Start Work

Subscribe to task event queue and start work, keeping track of which queues each worker services.

```
startWork (W1, Q1)

        WorkMap_W1 : Set
        +----------------------+
        | +----+ +----+        |
        | |    | |   *|        | (1)
        | |Q0  | |Q1  |        |
        | +----+ +----+        |
        +----------------------+

 1) SADD WorkMap_W1 Q1
```

### Stop Work

Unsubscribe from new task event queue. In-progress work is returned to the queue.

```
stopWork (W1, Q1)

        WorkMap_W1 : Set
        +----------------------+
        | +----+ +----+        |
        | |    | |   X|        | (1)
        | |Q0  | |Q1  |        |
        | +----+ +----+        |
        +----------------------+

 1) SREM WorkMap_W1 Q1
 2) perform steps 4-5 of failWorker
```

### Fail Worker

Stop all work and delete the worker. In-progress work is returned to the queue.

```
failWorker (W1)

        Workers : Set
    (1) +----------------------+  2) W1 = NULL
        | +----+ +----+        |
        | |    | |   X|        |
        | |W0  | |W1  |        |
        | +----+ +----+        |
        +----------------------+

 1) SREM Workers W1
 2) DEL W1
 3) SMEMBERS WorkMap_W1
 4) for each queue serviced by worker W1 (items in WorkMap_W1)
    SMEMBERS W1_Q1  (get all in-process tasks)
    DEL WorkMap_W1
        Q1 : List                         P1 : List
        +----------------------+          +--------+
        | +----+ +----+        |          | +----+X| (4a2)
        | |    | |   *|        | (4a1)    | |   X| |
        | |T1  | |T0  |        | <--T0<-- | |T0  | |
        | +----+ +----+        |          | +----+ |
        +----------------------+          +--------+
         head              tail
  a) for each in-process task (items in W1_Q1):
   1) RPOPLPUSH P1 Q1
   2) DEL P1
        W1_Q1 : Set
        +---------------------+
        | +----+ +----+      X| (4b)
        | |    | |    |       |
        | |P1  | |P0  |       |
        | +----+ +----+       |
        +---------------------+

  b) DEL W1_Q1
  c) if tasks exist for queue
     PUBLISH Q1_work count
```

* diagrams made with http://asciiflow.com/
