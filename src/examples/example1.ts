import { Worqr } from '../index';

const log = require('debug')('worqr:client');

const worqr = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), options: { password: process.env.REDIS_PASSWORD } });

const queueName = 'myQueue';
const workerNames = ['myWorker1', 'myWorker2', 'myWorker3', 'myWorker4'];

Promise.resolve()
    .then(() => Promise.all(workerNames.map(workerName => worqr.startWorker(workerName))))
    .then(() => Promise.all(workerNames.map(workerName => worqr.startWork(workerName, queueName))))
    .catch(console.error);

worqr.on(queueName, event => {
    switch (event.type) {
        case 'work':
            // const workerName = workerNames[Math.floor(Math.random() * workerNames.length)];

            workerNames.forEach(workerName => {
                Promise.resolve()
                    .then(() => worqr.startTask(queueName, workerName))
                    .then(([processName, task]) => {
                        if (!task) return log(`${workerName} did not get any tasks`);

                        log(`${workerName} doing ${task}`);

                        setTimeout(() => {
                            worqr.finishTask(processName as string);
                        }, Math.random() * 5000);
                    })
                    .catch(console.error);
            });
            break;
        case 'cancel':
            const task = event.message;

            Promise.resolve()
                .then(() => worqr.getProcessesForTask(task))
                .then(processNames => Promise.all(processNames.map(processName => {
                    log(`finishing ${processName}`);

                    return worqr.stopTask(processName);
                })))
                .catch(console.error);
            break;
    }
});

(function createRandomTask() {
    setTimeout(() => {
        const task = `task #${Math.round(Math.random() * 100)}`;

        Promise.resolve()
            .then(() => worqr.enqueue(queueName, task))
            .then(() => {
                // if (Math.random() > 0.5) {
                //     setTimeout(() => {
                //         worqr.cancelTasks(queueName, task);
                //     }, 500);
                // }
            })
            .catch(console.error);
        createRandomTask();
    }, Math.random() * 5000);
})();

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
//     // create workers
//     .then(() => worqr.startWorker('myWorker1'))
//     .then(() => worqr.startWorker('myWorker2'))
//     .then(() => worqr.startWorker('myWorker3'))
//     // fail to start task on unsubscribed queue
//     .then(() => worqr.startTask('myQueue1', 'myWorker1').catch(console.error))
//     // start worker 1 on all queues
//     .then(() => worqr.startWork('myWorker1', 'myQueue1'))
//     .then(() => worqr.startWork('myWorker1', 'myQueue2'))
//     .then(() => worqr.startWork('myWorker1', 'myQueue3'))
//     // start worker 1 tasks
//     .then(() => worqr.startTask('myQueue1', 'myWorker1'))
//     .then(() => worqr.startTask('myQueue2', 'myWorker1'))
//     .then(() => worqr.startTask('myQueue3', 'myWorker1'))
//     // test stopping and starting a task
//     .then(processName => worqr.stopTask(processName))
//     .then(() => worqr.startTask('myQueue3', 'myWorker1'))
//     // stop worker 1 on queue 2 and 3
//     .then(() => worqr.stopWork('myWorker1', 'myQueue2'))
//     .then(() => worqr.stopWork('myWorker1', 'myQueue3'))
//     // start worker 2 on queue 2
//     .then(() => worqr.startWork('myWorker2', 'myQueue2'))
//     // start worker 3 on queue 3
//     .then(() => worqr.startWork('myWorker3', 'myQueue3'))
//     // start worker 2 and 3 tasks
//     .then(() => worqr.startTask('myQueue2', 'myWorker2'))
//     .then(() => worqr.startTask('myQueue3', 'myWorker3'))
//     // stop worker 2
//     .then(() => worqr.stopWork('myWorker2', 'myQueue2'))
//     // fail worker 3
//     .then(() => worqr.failWorker('myWorker3'))
//     // start worker 2 on queue 3
//     .then(() => worqr.startWork('myWorker2', 'myQueue3'))
//     // test finishing a task
//     .then(() => worqr.startTask('myQueue3', 'myWorker2'))
//     .then(processName => worqr.finishTask(processName))
//     // start and finish another task
//     .then(() => worqr.startTask('myQueue3', 'myWorker2'))
//     .then(processName => worqr.finishTask(processName))
//     // verify the queue exists
//     .then(() => worqr.isQueue('myQueue3'))
//     .then(exists => console.log(`myQueue3 exists: ${exists}`))
//     // start and finish the last task
//     .then(() => worqr.startTask('myQueue3', 'myWorker2'))
//     .then(processName => worqr.finishTask(processName))
//     // verify the queue does not exist
//     .then(() => worqr.isQueue('myQueue3'))
//     .then(exists => console.log(`myQueue3 exists: ${exists}`))
//     // stop working on the empty queue 3
//     .then(() => worqr.stopWork('myWorker2', 'myQueue3'))
//     .catch(console.error);
