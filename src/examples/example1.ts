import { Worqr } from '../index';

const log = require('debug')('worqr:client');

const worqr = new Worqr(
    {
        host: <string>process.env.REDIS_HOST,
        port: Number.parseInt(<string>process.env.REDIS_PORT),
        password: <string>process.env.REDIS_PASSWORD
    },
    {
        redisKeyPrefix: 'myWorqr'
    });
const worqr1 = new Worqr(
    {
        host: <string>process.env.REDIS_HOST,
        port: Number.parseInt(<string>process.env.REDIS_PORT),
        password: <string>process.env.REDIS_PASSWORD
    },
    {
        redisKeyPrefix: 'myWorqr'
    });
const worqr2 = new Worqr(
    {
        host: <string>process.env.REDIS_HOST,
        port: Number.parseInt(<string>process.env.REDIS_PORT),
        password: <string>process.env.REDIS_PASSWORD
    },
    {
        redisKeyPrefix: 'myWorqr'
    });
const worqr3 = new Worqr(
    {
        host: <string>process.env.REDIS_HOST,
        port: Number.parseInt(<string>process.env.REDIS_PORT),
        password: <string>process.env.REDIS_PASSWORD
    },
    {
        redisKeyPrefix: 'myWorqr'
    });

const queueName = 'myQueue';

Promise.resolve()
    .then(() => worqr1.startWorker())
    .then(() => worqr2.startWorker())
    .then(() => worqr3.startWorker())
    .then(() => worqr1.startWork(queueName))
    .then(() => worqr2.startWork(queueName))
    .then(() => worqr3.startWork(queueName))
    .catch(console.error);

worqr1.on(queueName, (event: any) => handleEvent(worqr1, event));
worqr2.on(queueName, (event: any) => handleEvent(worqr2, event));
worqr3.on(queueName, (event: any) => handleEvent(worqr3, event));

function handleEvent(worqr: Worqr, { type, message }: { type: string, message: string }) {
    switch (type) {
        case 'work':
            Promise.resolve()
                .then(() => worqr.startTask(queueName))
                .then(([processName, task]) => {
                    if (!task) return log(`${worqr.getWorkerId()} did not get any tasks`);

                    log(`${worqr.getWorkerId()} doing ${task}`);

                    setTimeout(() => {
                        worqr.finishTask(processName as string);
                    }, Math.random() * 5000);
                })
                .catch(console.error);
            break;
        case 'cancel':
            const task = message;

            Promise.resolve()
                .then(() => worqr.getMatchingProcesses(task))
                .then(processNames => Promise.all(processNames.map(processName => {
                    log(`finishing ${processName}`);

                    return worqr.stopTask(processName);
                })))
                .catch(console.error);
            break;
        case 'delete':
            Promise.resolve()
                .then(() => worqr.stopWork(queueName))
                .catch(console.error);
    }
}

(function createRandomTask() {
    setTimeout(() => {
        const task1 = `task #${Math.round(Math.random() * 100)}`;
        const task2 = `task #${Math.round(Math.random() * 100)}`;
        const task3 = `task #${Math.round(Math.random() * 100)}`;

        Promise.resolve()
            .then(() => worqr.enqueue(queueName, task1))
            .then(() => worqr.enqueue(queueName, task2))
            .then(() => worqr.enqueue(queueName, task3))
            .catch(console.error);
        createRandomTask();
    }, Math.random() * 5000);
})();

// setTimeout(() => {
//     worqr.deleteQueue(queueName);
// });

// Promise.resolve()
//     // create work queues
//     .then(() => Promise.all([
//         worqr.enqueue('myQueue1', 'myTask1'),
//         worqr.enqueue('myQueue1', 'myTask2'),
//         worqr.enqueue('myQueue1', 'myTask3'),

//         worqr.enqueue('myQueue2', 'myTask1'),
//         worqr.enqueue('myQueue2', 'myTask2'),
//         worqr.enqueue('myQueue2', 'myTask3'),

//         worqr.enqueue('myQueue3', 'myTask1'),
//         worqr.enqueue('myQueue3', 'myTask2'),
//         worqr.enqueue('myQueue3', 'myTask3')
//     ]))
//     // verify queue
//     .then(() => worqr.getQueueNames())
//     .then(queues => console.log(`queues: ${queues.toString()}`))
//     // start worker
//     .then(() => worqr.startWorker())
//     // fail to start task on unsubscribed queue
//     .then(() => worqr.startTask('myQueue1').catch(console.error))
//     // start work on all queues
//     .then(() => worqr.startWork('myQueue1'))
//     .then(() => worqr.startWork('myQueue2'))
//     .then(() => worqr.startWork('myQueue3'))
//     // start tasks
//     .then(() => worqr.startTask('myQueue1'))
//     // test stopping and starting a task
//     .then(([processName, task]) => worqr.stopTask(processName as string))
//     .then(() => worqr.startTask('myQueue1'))
//     // stop work on queue 2
//     .then(() => worqr.stopWork('myQueue2'))
//     // test finishing a task
//     .then(() => worqr.startTask('myQueue3'))
//     .then(([processName, task]) => worqr.finishTask(processName as string))
//     // start and finish another task
//     .then(() => worqr.startTask('myQueue3'))
//     .then(([processName, task]) => worqr.finishTask(processName as string))
//     // verify the queue exists
//     .then(() => worqr.isQueue('myQueue3'))
//     .then(exists => console.log(`myQueue3 exists: ${exists}`))
//     // start and finish the last task
//     .then(() => worqr.startTask('myQueue3'))
//     .then(([processName, task]) => worqr.finishTask(processName as string))
//     // verify the queue does not exist
//     .then(() => worqr.isQueue('myQueue3'))
//     .then(exists => console.log(`myQueue3 exists: ${exists}`))
//     // stop working on the empty queue 3
//     .then(() => worqr.stopWork('myQueue3'))
//     .catch(console.error);
