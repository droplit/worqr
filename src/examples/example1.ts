import { Worqr } from '../index';

const log = require('debug')('worqr:client');

const worqr = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), password: <string>process.env.REDIS_PASSWORD }, { redisKeyPrefix: 'myWorqr' });
const worqr1 = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), password: <string>process.env.REDIS_PASSWORD }, { redisKeyPrefix: 'myWorqr' });
const worqr2 = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), password: <string>process.env.REDIS_PASSWORD }, { redisKeyPrefix: 'myWorqr' });
const worqr3 = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), password: <string>process.env.REDIS_PASSWORD }, { redisKeyPrefix: 'myWorqr' });

const queueName = 'myQueue';

worqr1.on(queueName, (event: any) => handleEvent(worqr1, event));
worqr2.on(queueName, (event: any) => handleEvent(worqr2, event));
worqr3.on(queueName, (event: any) => handleEvent(worqr3, event));

function handleEvent(worqr: Worqr, { type, message }: { type: string, message: string }) {
    switch (type) {
        case 'work':
            Promise.resolve()
                .then(() => worqr.startTask(queueName))
                .then(([processName, task]) => {
                    if (!processName || !task) return log(`${worqr.getWorkerId()} did not get any tasks`);

                    log(`${worqr.getWorkerId()} doing ${task}`);

                    setTimeout(() => {
                        worqr.finishTask(processName);
                    }, Math.random() * 5000);
                })
                .catch(console.error);
            break;
        case 'cancel':
            const task = message;

            Promise.resolve()
                .then(() => worqr.getMatchingProcesses(task))
                .then(processNames => Promise.all(processNames.map(processName => {
                    log(`${worqr.getWorkerId()} finishing ${processName}`);

                    return worqr.stopTask(processName);
                })))
                .catch(console.error);
            break;
        case 'delete':
            Promise.resolve()
                .then(() => worqr.stopWork(queueName))
                .then(() => {
                    log(`${worqr.getWorkerId()} stopped work on ${queueName}`);
                })
                .catch(console.error);
            break;
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

        Promise.all([
            worqr.enqueue(queueName, task1),
            worqr.enqueue(queueName, task2),
            worqr.enqueue(queueName, task3)
        ])
            .catch(console.error);

        createRandomTask();
    }, Math.random() * 5000);
})();

// setTimeout(() => {
//     worqr.deleteQueue(queueName)
//         .catch(console.error);
// }, 10000);

// setTimeout(() => {
//     Promise.all([
//         worqr1.startWork(queueName),
//         worqr2.startWork(queueName),
//         worqr3.startWork(queueName)
//     ])
//         .catch(console.error);
// }, 15000);
