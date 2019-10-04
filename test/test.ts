import { expect } from 'chai';
import { Worqr } from '../src';

let worqr: Worqr;
let processId: string;

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
        Promise.resolve()
            .then(() => worqr.enqueue('queue1', ['task1', 'task2', 'task3']))
            .then(() => done())
            .catch(done);
    });

    it('should enqueue three tasks to a second queue', done => {
        Promise.resolve()
            .then(() => worqr.enqueue('queue2', ['task1', 'task2', 'task3']))
            .then(() => done())
            .catch(done);
    });

    it('should enqueue three tasks to a third queue', done => {
        Promise.resolve()
            .then(() => worqr.enqueue('queue3', ['task1', 'task2', 'task3']))
            .then(() => done())
            .catch(done);
    });

    it('should verify there are three queues', done => {
        worqr.getQueues()
            .then(queueNames => {
                expect(queueNames).to.include('queue1');
                expect(queueNames).to.include('queue2');
                expect(queueNames).to.include('queue3');
                done();
            })
            .catch(done);
    });

    it('should fail to start work on the first queue', done => {
        worqr.startWork('queue1')
            .then(() => done(new Error('Worker trying to start work on a queue before')))
            .catch(() => done());
    });

    it('should start the worker', done => {
        worqr.startWorker()
            .then(() => done())
            .catch(done);
    });

    it('should list all workers', done => {
        worqr.getWorkers()
            .then(workerIds => {
                expect(workerIds).to.have.lengthOf(1);
                expect(workerIds).to.include(worqr.getWorkerId());
                done();
            })
            .catch(done);
    });

    it('should fail to start task on the first queue', done => {
        worqr.dequeue('queue1')
            .then(() => done(new Error('Shouldn\'t be able to start task')))
            .catch(() => done());
    });

    it('should start work on the first queue', done => {
        worqr.startWork('queue1')
            .then(() => done())
            .catch(done);
    });

    it('should ensure there are three tasks in the first queue', done => {
        worqr.countQueue('queue1')
            .then(count => {
                expect(count).to.equal(3);
                done();
            })
            .catch(done);
    });

    it('should peek the first queue', done => {
        worqr.peekQueue('queue1')
            .then(task => {
                expect(task).to.exist;
                expect(task).to.equal('task1');
                done();
            })
            .catch(done);
    });

    it('should list all the tasks in the first queue', done => {
        worqr.getTasks('queue1')
            .then(tasks => {
                expect(tasks).to.have.lengthOf(3);
                expect(tasks).to.include('task1');
                expect(tasks).to.include('task2');
                expect(tasks).to.include('task3');
                done();
            })
            .catch(done);
    });

    it('should start a task on the first queue', done => {
        worqr.dequeue('queue1')
            .then(process => {
                expect(process).to.exist;
                if (process) {
                    expect(process.id).to.exist;
                    expect(process.task).to.exist;
                    expect(process.task).to.equal('task1');
                    processId = process.id;
                }
                done();
            })
            .catch(done);
    });

    it('should finish the process', done => {
        worqr.finishProcess(processId)
            .then(() => done())
            .catch(done);
    });

    it('should start another task', done => {
        worqr.dequeue('queue1')
            .then(process => {
                expect(process).to.exist;
                if (process) {
                    expect(process.id).to.exist;
                    expect(process.task).to.exist;
                    expect(process.task).to.equal('task2');
                    processId = process.id;
                }
                done();
            })
            .catch(done);
    });

    it('should stop the process', done => {
        worqr.stopProcess(processId)
            .then(() => done())
            .catch(done);
    });

    it('should ensure there are two tasks in the first queue', done => {
        worqr.countQueue('queue1')
            .then(count => {
                expect(count).to.equal(2);
                done();
            })
            .catch(done);
    });

    it('should subscribe to the first queue, enqueue a task, and get a work event', done => {
        function listener(type: string, message: string) {
            expect(type).to.equal('work');
            worqr.removeListener('queue1', listener);
            done();
        }

        worqr.addListener('queue1', listener);

        worqr.enqueue('queue1', 'task4')
            .catch(done);
    });

    it('should finish three tasks on the first queue', done => {
        Promise.resolve()
            .then(() => worqr.dequeue('queue1'))
            .then(process => {
                expect(process).to.exist;
                if (process)
                    return worqr.finishProcess(process.id);
                else
                    return Promise.resolve();
            })
            .then(() => worqr.dequeue('queue1'))
            .then(process => {
                expect(process).to.exist;
                if (process)
                    return worqr.finishProcess(process.id);
                else
                    return Promise.resolve();
            })
            .then(() => worqr.dequeue('queue1'))
            .then(process => {
                expect(process).to.exist;
                if (process)
                    return worqr.finishProcess(process.id);
                else
                    return Promise.resolve();
            })
            .then(() => done())
            .catch(done);
    });

    it('should ensure the first queue is empty', done => {
        worqr.peekQueue('queue1')
            .then(task => {
                expect(task).to.not.exist;
                done();
            })
            .catch(done);
    });

    it('should subscribe to the first queue, enqueue a task, cancel the task, receive a cancel event, and stop the process', done => {
        function listener(type: string, message: string) {
            if (type === 'cancel') {
                Promise.resolve()
                    .then(() => worqr.getMatchingProcesses(message))
                    .then(processNames => processNames.map(processName => worqr.stopProcess(processName)))
                    .then(() => {
                        worqr.removeListener('queue1', listener);
                        done();
                    })
                    .catch(done);
            }
        }

        worqr.addListener('queue1', listener);

        Promise.resolve()
            .then(() => worqr.enqueue('queue1', 'task1'))
            .then(() => worqr.cancelTasks('queue1', 'task1'))
            .catch(done);
    });

    it('should subscribe to the first queue, delete the queue, and receive a delete event, and stop work on the queue', done => {
        function listener(type: string, message: string) {
            if (type === 'delete') {
                worqr.stopWork('queue1')
                    .then(() => {
                        worqr.removeListener('queue1', listener);
                        done();
                    })
                    .catch(done);
            }
        }

        worqr.addListener('queue1', listener);

        worqr.deleteQueue('queue1')
            .catch(done);
    });

    it('should fail the worker', done => {
        worqr.failWorker()
            .then(() => done())
            .catch(done);
    });

    it('should delete the other two queues', done => {
        Promise.all([
            worqr.deleteQueue('queue2'),
            worqr.deleteQueue('queue3')
        ])
            .then(() => done())
            .catch(done);
    });
});
