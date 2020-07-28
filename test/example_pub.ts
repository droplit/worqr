import debug from 'debug';
import { Worqr } from '../src';

const host = process.env.REDIS_HOST as string;
const port = Number(process.env.REDIS_PORT as string);
const password = process.env.REDIS_PASSWORD as string;
const redisKeyPrefix = 'worqr.example';
const queueName = 'queue';

// responsible for putting tasks on the queue periodically
const worqr = new Worqr({ host, port, password }, { redisKeyPrefix });
const worqrLog = debug(`worqr:${worqr.getWorkerId()}`);

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
            .then(() => {
                worqrLog(`sucessfully enqueued tasks`);
            })
            .catch(console.error);
        createRandomTask();
    }, Math.random() * 5000);
})();

process.on('SIGINT', () => {
    worqr.failWorker().then(() => process.exit());
});
