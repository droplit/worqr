import { Worqr } from '../src';

const log = require('debug')('worqr:client');

// in this example, the main worker is just responsible for putting tasks on the queue periodically
// the other 3 are set up to pull work off of the queue
const worqr = new Worqr({
    host: process.env.REDIS_HOST as string,
    port: Number.parseInt(process.env.REDIS_PORT as string),
    password: process.env.REDIS_PASSWORD as string
}, { redisKeyPrefix: 'worqr.example' });
const worqr1 = new Worqr({
    host: process.env.REDIS_HOST as string,
    port: Number.parseInt(process.env.REDIS_PORT as string),
    password: process.env.REDIS_PASSWORD as string
}, { redisKeyPrefix: 'worqr.example' });
const worqr2 = new Worqr({
    host: process.env.REDIS_HOST as string,
    port: Number.parseInt(process.env.REDIS_PORT as string),
    password: process.env.REDIS_PASSWORD as string
}, { redisKeyPrefix: 'worqr.example' });
const worqr3 = new Worqr({
    host: process.env.REDIS_HOST as string,
    port: Number.parseInt(process.env.REDIS_PORT as string),
    password: process.env.REDIS_PASSWORD as string
}, { redisKeyPrefix: 'worqr.example' });

const queueName = 'queue';

worqr1.on(queueName, (type: string, message: string) => handleEvent(worqr1, type, message));
worqr2.on(queueName, (type: string, message: string) => handleEvent(worqr2, type, message));
worqr3.on(queueName, (type: string, message: string) => handleEvent(worqr3, type, message));

function handleEvent(worqr: Worqr, type: string, message: string) {
    switch (type) {
        // indicates that work has been added to the queue
        // the worker should start a task on the queue
        case 'work': {
            const count = Number(message);
            log(`${worqr.getWorkerId()}: ${count} tasks have been added to ${queueName}`);
            for (let i = 0; i < count; i++) {
                Promise.resolve()
                    .then(() => worqr.dequeue(queueName))
                    .then(process => {
                        if (!process) return log(`${worqr.getWorkerId()}: did not get any tasks`);

                        log(`${worqr.getWorkerId()}: doing ${process.task}`);

                        // simulate a long async task
                        setTimeout(() => {
                            log(`${worqr.getWorkerId()}: finished ${process.task}`);

                            worqr.finishProcess(process.id);

                            // ask for more work
                            worqr.requestWork(queueName);
                        }, Math.random() * 5000);
                    })
                    .catch(console.error);
            }
            break;
        }
        // indicates that a task was finished by anybody
        case 'done': {
            const { workerId, task } = JSON.parse(message);
            log(`${worqr.getWorkerId()}: ${workerId} finished ${task}`);
            break;
        }
        // indicates that a certain task has been cancelled
        // the worker should get all process names matching the task and cancel them
        case 'cancel': {
            const task = message;
            Promise.resolve()
                .then(() => worqr.getMatchingProcesses(task))
                .then(processIds => Promise.all(processIds.map(processId => {
                    log(`${worqr.getWorkerId()}: stopping ${processId}`);

                    return worqr.stopProcess(processId);
                })))
                .catch(console.error);
            break;
        }
        // indicates that the queue has been deleted
        // the worker should stop work on the queue
        case 'delete': {
            Promise.resolve()
                .then(() => worqr.stopWork(queueName))
                .then(() => {
                    log(`${worqr.getWorkerId()}: stopped work on ${queueName}`);
                })
                .catch(console.error);
            break;
        }
    }
}

Promise.resolve()
    .then(() => Promise.all([
        worqr1.startWorker(),
        worqr2.startWorker(),
        worqr3.startWorker()
    ]))
    .then(() => Promise.all([
        worqr1.startWork(queueName),
        worqr2.startWork(queueName),
        worqr3.startWork(queueName)
    ]))
    .catch(console.error);

(function createRandomTask() {
    setTimeout(() => {
        const task1 = `task #${Math.round(Math.random() * 100)}`;
        const task2 = `task #${Math.round(Math.random() * 100)}`;
        const task3 = `task #${Math.round(Math.random() * 100)}`;
        const task4 = ''; // make sure empty string task works correctly
        const tasks = [`task #${Math.round(Math.random() * 100)}`, `task #${Math.round(Math.random() * 100)}`]; // make sure task arrays work correctly

        Promise.all([
            worqr.enqueue(queueName, task1),
            worqr.enqueue(queueName, task2),
            worqr.enqueue(queueName, task3),
            worqr.enqueue(queueName, task4),
            worqr.enqueue(queueName, tasks)
        ])
            .catch(console.error);

        createRandomTask();
    }, Math.random() * 5000);
})();

// setTimeout(() => {
//     worqr.deleteQueue(queueName)
//         .catch(console.error);
// }, 10000);
