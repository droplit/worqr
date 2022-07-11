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

// responsible for putting tasks on the queue periodically
const worqr = new Worqr(options);
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
