# Worqr

A distributed, reliable, job queueing system that uses redis as a backend.

- By Droplit

Requirements:

- Only Redis is required as a back-end server. No server components to install. Operates purely peer-to-peer in your application.
- Every transaction is fully atomic (queue never left in inconsistent state)
- When workers fail, all tasks are put back into the queue at the front
- Queue doesn't keep any item history
- As a work queue, each item is only delivered to exactly one worker
- work items are not enveloped, so there is no encoding/decoding overhead or chance for the format to change in future releases.
- uses pub/sub for new work notifications

## Theory of Operation

The primary unit of work to be done is a **Task**. Tasks are organized into **Queues**. Queues are serviced by **Workers**.

## Terminology

**Task** - something a worker consumes.

**Worker** - a distributed agent that consumes tasks.

**Queue** - a first in, first out list of Tasks.

## Diagrams

### Enqueue

Add a **Task** to a **Queue**.

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

Move a **Task** from a "to do" list to a "doing" list.

In the diagram, `W1` refers to the worker.

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

Removes a **Task** from a **Worker**'s personal "to do" list.

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

Moves a **Task** from a **Worker**'s list into a top level "to do" list.

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

Workers listen on Q1* and "finish" all tasks that are cancelled.

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

Register **Worker** as existing and available for work.

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

```
keepWorkerAlive (W1)

 1) SET W1 RUN EX 3 XX  (XX = set if exists)

    If SET fails, worker has failed to stay alive and must restart
```

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

### Start Work

Subscribe to Task event queue and start work. Keeps track of which queues each worker services.

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

Unsubscribe from new **Task** event queue. Assume anything remaining should return to todo.

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

Stop all tasks.

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

## Development

### Setup

Install dependencies

```
npm install
```

Create `.env` file and input environment variables. See `.example.env for reference`.

### Build

Build and transpile TS

```
npm run build
```

### Lint code

```
npm run tslint
```

### Run tests

```
npm test
```

### Run example

Example script to test functionality

```
npm run example
```
