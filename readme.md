# Worqr

A distributed, reliable, job queueing system that uses redis as a backend.

- By Droplit

Requirements:
* Only Redis is required as a back-end server. No server components to install. Operates purely peer-to-peer in your application.
* Every transaction is fully atomic (queue never left in inconsistent state)
* When workers fail, all tasks are put back into the queue at the front
* Queue doesn't keep any item history
* As a work queue, each item is only delivered to exactly one worker
* work items are not enveloped, so there is no encoding/decoding overhead or chance for the format to change in future releases.
* uses pub/sub for new work notifications

## Theory of Operation
The primary unit of work to be done is a **Task**. Tasks are organized into **Queues**. Queues are serviced by **Workers**.

#### Enqueue
Add a work item to a named queue.

In this example, the queue name is `Q1` and the task payload is `T1`

`LPUSH Q1 T1`
```
        Q1 : List
        +----------------------+
        | +----+ +----+        |
        | |   *| |    |        |
T1 +--> | |T1  | |T0  |        |
        | +----+ +----+        |
        +----------------------+
         head              tail

        Q1 : List                         P1 : List           W1_Q1
        +----------------------+          +--------+          +---------------------+
        | +----+ +----+        |          | +----+ |          | +----+ +----+       |
        | |    | |   X|        |          | |   *| |          | |   *| |    |       |
        | |T1  | |T0  |        |  T0 +--> | |T0  | |  P1 +--> | |P1  | |P0  |       |
        | +----+ +----+        |          | +----+ |          | +----+ +----+       |
        +----------------------+          +--------+          +---------------------+
         head              tail                                head             tail

```