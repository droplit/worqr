import { expect } from 'chai';

import { Worqr, QueueEvent } from './';

let worqr: Worqr;
let processName: string;

describe('Worqr', function () {
    it('should create a new Worqr instance', () => {
        worqr = new Worqr({
            host: process.env.REDIS_HOST as string,
            port: Number.parseInt(process.env.REDIS_PORT as string),
            password: process.env.REDIS_PASSWORD as string
        });

        expect(worqr).to.exist;
    });

    it('should clean up any existing workers', done => {
        worqr.cleanupWorkers()
            .then(() => done())
            .catch(done);
    });

    it('should enqueue three tasks to a queue', done => {
        Promise.all([
            worqr.enqueue('queue1', 'task1'),
            worqr.enqueue('queue1', 'task2'),
            worqr.enqueue('queue1', 'task3')
        ])
            .then(() => done())
            .catch(done);
    });

    it('should enqueue three tasks to a second queue', done => {
        Promise.all([
            worqr.enqueue('queue2', 'task1'),
            worqr.enqueue('queue2', 'task2'),
            worqr.enqueue('queue2', 'task3')
        ])
            .then(() => done())
            .catch(done);
    });

    it('should enqueue three tasks to a third queue', done => {
        Promise.all([
            worqr.enqueue('queue3', 'task1'),
            worqr.enqueue('queue3', 'task2'),
            worqr.enqueue('queue3', 'task3')
        ])
            .then(() => done())
            .catch(done);
    });

    it('should verify there are three queues', done => {
        worqr.getQueueNames()
            .then(queueNames => {
                expect(queueNames).to.include('queue1');
                expect(queueNames).to.include('queue2');
                expect(queueNames).to.include('queue3');
                done();
            })
            .catch(done);
    });

    it('should start the worker', done => {
        worqr.startWorker()
            .then(() => done())
            .catch(done);
    });

    it('should fail to start task on the first queue', done => {
        worqr.startTask('queue1')
            .catch(() => done());
    });

    it('should start work on the first queue', done => {
        worqr.startWork('queue1')
            .then(() => done())
            .catch(done);
    });

    it('should ensure the first queue exists', done => {
        worqr.isQueue('queue1')
            .then(isQueue => {
                expect(isQueue).to.be.true;
                done();
            })
            .catch(done);
    });

    it('should ensure there are three tasks in the first queue', done => {
        worqr.getQueueCount('queue1')
            .then(count => {
                expect(count).to.equal(3);
                done();
            })
            .catch(done);
    });

    it('should start a task on the first queue', done => {
        worqr.startTask('queue1')
            .then(process => {
                expect(process).to.exist;
                if (process) {
                    expect(process.processName).to.exist;
                    expect(process.task).to.exist;
                    expect(process.task).to.equal('task1');
                    processName = process.processName;
                }
                done();
            })
            .catch(done);
    });

    it('should finish the task', done => {
        worqr.finishProcess(processName)
            .then(() => done())
            .catch(done);
    });

    it('should ensure there are two tasks in the first queue', done => {
        worqr.getQueueCount('queue1')
            .then(count => {
                expect(count).to.equal(2);
                done();
            })
            .catch(done);
    });

    it('should listen for work on the first queue, enqueue a task, and get a work event', done => {
        const listener = (event: QueueEvent) => {
            expect(event.type).to.equal('work');
            worqr.removeListener('queue1', listener);
            done();
        };

        worqr.addListener('queue1', listener);
        worqr.enqueue('queue1', 'task4');
    });

    it('should finish three tasks on the first queue', done => {
        Promise.resolve()
            .then(() => worqr.startTask('queue1'))
            .then(process => {
                expect(process).to.exist;
                if (process)
                    return worqr.finishProcess(process.processName);
                else
                    return Promise.resolve();
            })
            .then(() => worqr.startTask('queue1'))
            .then(process => {
                expect(process).to.exist;
                if (process)
                    return worqr.finishProcess(process.processName);
                else
                    return Promise.resolve();
            })
            .then(() => worqr.startTask('queue1'))
            .then(process => {
                expect(process).to.exist;
                if (process)
                    return worqr.finishProcess(process.processName);
                else
                    return Promise.resolve();
            })
            .then(() => done())
            .catch(done);
    });

    it('should ensure the first queue is empty', done => {
        worqr.isQueue('queue1')
            .then(isQueue => {
                expect(isQueue).to.be.false;
                done();
            })
            .catch(done);
    });

    it('should stop work on the first queue', done => {
        worqr.stopWork('queue1')
            .then(() => done())
            .catch(done);
    });

    it('should fail the worker', done => {
        worqr.failWorker()
            .then(() => done())
            .catch(done);
    });

    it('should delete the three queues', done => {
        Promise.all([
            worqr.deleteQueue('queue1'),
            worqr.deleteQueue('queue2'),
            worqr.deleteQueue('queue3')
        ])
            .then(() => done())
            .catch(done);
    });
});
