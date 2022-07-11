import debug from 'debug';
import { Worqr } from '../src';

const queueName = 'queue';

const options = {
    redis: {
        redisClientOptions: {
            url: process.env.REDIS_URL,
            password: process.env.REDIS_PASSWORD
        }
    },
    worqr: {
        redisKeyPrefix: 'worqr.example'
    }
}

// workers to pull work off of the queue
const worqr1 = new Worqr(options);
const worqr1Log = debug(`worqr:${worqr1.getWorkerId()}`);
worqr1.on(queueName, (type: string, message: string) => handleEvent(worqr1, worqr1Log, type, message));
worqr1.on('debug', event => worqr1Log(`DEBUG: %O`, event));
worqr1.on('error', err => worqr1Log(`ERROR: %O`, err));

const worqr2 = new Worqr(options);
const worqr2Log = debug(`worqr:${worqr2.getWorkerId()}`);
worqr2.on(queueName, (type: string, message: string) => handleEvent(worqr2, worqr2Log, type, message));
worqr2.on('debug', event => worqr2Log(`DEBUG: %O`, event));
worqr2.on('error', err => worqr2Log(`ERROR: %O`, err));

const worqr3 = new Worqr(options);
const worqr3Log = debug(`worqr:${worqr3.getWorkerId()}`);
worqr3.on(queueName, (type: string, message: string) => handleEvent(worqr3, worqr3Log, type, message));
worqr3.on('debug', event => worqr3Log(`DEBUG: %O`, event));
worqr3.on('error', err => worqr3Log(`ERROR: %O`, err));

function handleEvent(worqr: Worqr, worqrLog: debug.Debugger, type: string, message: string) {
    switch (type) {
        // indicates that work has been added to the queue
        // the worker should start a task on the queue
        case 'work': {
            const count = Number(message);
            worqrLog(`${count} tasks have been added to ${queueName}`);
            for (let i = 0; i < count; i++) {
                Promise.resolve()
                    .then(() => worqr.dequeue(queueName))
                    .then(process => {
                        if (!process) {
                            return worqrLog(`did not get any tasks`);
                        }
                        worqrLog(`doing ${process.task}`);
                        // simulate a long async task
                        setTimeout(() => {
                            worqrLog(`finished ${process.task}`);
                            worqr.finishProcess(process.id, `${process.task} completed`);
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
            const { workerId, task, result } = JSON.parse(message) as { workerId: string, task: string, result: any };
            worqrLog(`${workerId} finished ${task} with result: ${result}`);
            break;
        }
        // indicates that a certain task has been cancelled
        // the worker should get all process names matching the task and cancel them
        case 'cancel': {
            const task = message;
            Promise.resolve()
                .then(() => worqr.getMatchingProcesses(task))
                .then(processIds => Promise.all(processIds.map(processId => {
                    worqrLog(`stopping ${processId}`);
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
                    worqrLog(`stopped work on ${queueName}`);
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

process.on('SIGINT', () => {
    Promise.all([
        worqr1.failWorker(),
        worqr2.failWorker(),
        worqr3.failWorker()
    ]).then(() => process.exit());
});
