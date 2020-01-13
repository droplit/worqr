import { Worqr } from '../src';

const log = require('debug')('worqr:example:sub');

const host = process.env.REDIS_HOST as string;
const port = Number(process.env.REDIS_PORT as string);
const password = process.env.REDIS_PASSWORD as string;
const redisKeyPrefix = 'worqr.example';
const queueName = 'queue';

// workers to pull work off of the queue
const worqr1 = new Worqr({ host, port, password }, { redisKeyPrefix });
const worqr2 = new Worqr({ host, port, password }, { redisKeyPrefix });
const worqr3 = new Worqr({ host, port, password }, { redisKeyPrefix });

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
                        if (!process) {
                            return log(`${worqr.getWorkerId()}: did not get any tasks`);
                        }
                        log(`${worqr.getWorkerId()}: doing ${process.task}`);
                        // simulate a long async task
                        setTimeout(() => {
                            log(`${worqr.getWorkerId()}: finished ${process.task}`);
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
            log(`${worqr.getWorkerId()}: ${workerId} finished ${task} with result: ${result}`);
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
