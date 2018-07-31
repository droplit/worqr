import { Worqr } from '../index';

const log = require('debug')('worqr:client');

const worqr = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), password: <string>process.env.REDIS_PASSWORD }, { redisKeyPrefix: 'myWorqr' });

Promise.resolve()
    // create work queues
    .then(() => Promise.all([
        worqr.enqueue('myQueue1', 'myTask1'),
        worqr.enqueue('myQueue1', 'myTask2'),
        worqr.enqueue('myQueue1', 'myTask3'),

        worqr.enqueue('myQueue2', 'myTask1'),
        worqr.enqueue('myQueue2', 'myTask2'),
        worqr.enqueue('myQueue2', 'myTask3'),

        worqr.enqueue('myQueue3', 'myTask1'),
        worqr.enqueue('myQueue3', 'myTask2'),
        worqr.enqueue('myQueue3', 'myTask3')
    ]))
    // verify queue
    .then(() => worqr.getQueueNames())
    .then(queues => log(`queues: ${queues.toString()}`))
    // start worker
    .then(() => worqr.startWorker())
    // fail to start task on unsubscribed queue
    .then(() => worqr.startTask('myQueue1').catch(console.error))
    // start work on all queues
    .then(() => worqr.startWork('myQueue1'))
    .then(() => worqr.startWork('myQueue2'))
    .then(() => worqr.startWork('myQueue3'))
    // start tasks
    .then(() => worqr.startTask('myQueue1'))
    // test stopping and starting a task
    .then(process => process ? worqr.stopTask(process.processName) : Promise.resolve())
    .then(() => worqr.startTask('myQueue1'))
    // stop work on queue 2
    .then(() => worqr.stopWork('myQueue2'))
    // test finishing a task
    .then(() => worqr.startTask('myQueue3'))
    .then(process => process ? worqr.finishTask(process.processName) : Promise.resolve())
    // start and finish another task
    .then(() => worqr.startTask('myQueue3'))
    .then(process => process ? worqr.finishTask(process.processName) : Promise.resolve())
    // verify the queue exists
    .then(() => worqr.isQueue('myQueue3'))
    .then(exists => console.log(`myQueue3 exists: ${exists}`))
    // start and finish the last task
    .then(() => worqr.startTask('myQueue3'))
    .then(process => process ? worqr.finishTask(process.processName) : Promise.resolve())
    // verify the queue does not exist
    .then(() => worqr.isQueue('myQueue3'))
    .then(exists => console.log(`myQueue3 exists: ${exists}`))
    // stop working on the empty queue 3
    .then(() => worqr.stopWork('myQueue3'))
    .catch(console.error);