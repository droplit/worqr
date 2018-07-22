import { Worqr } from '../index';

const log = require('debug')('worqr:client');

const worqr = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), options: { password: process.env.REDIS_PASSWORD } });

const queueName = 'myQueue';

Promise.resolve()
    .then(() => worqr.startWorker())
    .then(() => worqr.startWork(queueName))
    .catch(console.error);

worqr.on(queueName, event => {
    switch (event.type) {
        case 'work':
            Promise.resolve()
                .then(() => worqr.startTask(queueName))
                .then(([processName, task]) => {
                    if (!task) return log(`did not get any tasks`);

                    log(`doing ${task}`);

                    setTimeout(() => {
                        worqr.finishTask(processName as string);
                    }, Math.random() * 5000);
                })
                .catch(console.error);
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
//     // start worker
//     .then(() => worqr.startWorker())
//     // fail to start task on unsubscribed queue
//     .then(() => worqr.startTask('myQueue1').catch(console.error))
//     // start all queues
//     .then(() => worqr.startWork('myQueue1'))
//     .then(() => worqr.startWork('myQueue2'))
//     .then(() => worqr.startWork('myQueue3'))
//     // start tasks
//     .then(() => worqr.startTask('myWorker1'))
//     // test stopping and starting a task
//     .then(([processName, task]) => worqr.stopTask(processName as string))
//     .then(() => worqr.startTask('myQueue3'))
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
