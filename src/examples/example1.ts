import { Worqr } from '../index';

require('dotenv').config();

const worqr = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), options: { password: process.env.REDIS_PASSWORD } }, { redisKeyPrefix: 'worq1' });

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
    // verify queues
    .then(() => worqr.getQueueNames('myQueue1'))
    .then(console.log)
    // create workers
    .then(() => worqr.startWorker('myWorker1'))
    .then(() => worqr.startWorker('myWorker2'))
    .then(() => worqr.startWorker('myWorker3'))
    // start worker 1 on all queues
    .then(() => worqr.startWork('myWorker1', 'myQueue1'))
    .then(() => worqr.startTask('myQueue1', 'myWorker1'))
    .then(() => worqr.startWork('myWorker1', 'myQueue2'))
    .then(() => worqr.startTask('myQueue2', 'myWorker1'))
    .then(() => worqr.startWork('myWorker1', 'myQueue3'))
    .then(() => worqr.startTask('myQueue3', 'myWorker1'))
    // test stopping and starting a task
    .then(processName => worqr.stopTask(processName))
    .then(() => worqr.startTask('myQueue2', 'myWorker1'))
    // stop worker 1 on queue 2 and 3
    .then(() => worqr.stopWork('myWorker1', 'myQueue2'))
    .then(() => worqr.stopWork('myWorker1', 'myQueue3'))
    // start worker 2 on queue 2
    .then(() => worqr.startWork('myWorker2', 'myQueue2'))
    .then(() => worqr.startTask('myQueue2', 'myWorker2'))
    // start worker 3 on queue 3
    .then(() => worqr.startWork('myWorker3', 'myQueue3'))
    .then(() => worqr.startTask('myQueue3', 'myWorker3'))
    // stop worker 2
    .then(() => worqr.stopWork('myWorker2', 'myQueue2'))
    // fail worker 3
    .then(() => worqr.failWorker('myWorker3'))
    // start worker 2 on queue 3
    .then(() => worqr.startWork('myWorker2', 'myQueue3'))
    .then(() => worqr.startTask('myQueue3', 'myWorker2'))
    .catch(console.error);
