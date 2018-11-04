import { EventEmitter } from 'events';
import * as redis from 'redis';
import * as uuid from 'uuid';

import { RedisOptions, WorqrOptions, Process } from './types';

const log = require('debug')('worqr');

/**
 * Represents queues and processes that this worker is working on.
 */
interface WorkerItems {
    queueNames: string[];
    processIds: string[];
}

/**
 * Represents whether a worker is dead.
 */
interface WorkerStatus {
    workerId: string;
    dead: boolean;
}

/**
 * A distributed, reliable, job queueing system that uses redis as a backend.
 */
export class Worqr extends EventEmitter {
    private pub: redis.RedisClient;
    private sub: redis.RedisClient;
    private redisKeyPrefix = 'worqr';
    private workerId = uuid.v4();
    private workerHeartbeatInterval = 1000;
    private workerTimeout = 3;
    private workerCleanupInterval = 10000;
    private digestBiteSize = 0;
    private workerTimerInterval?: NodeJS.Timer;
    private queues: string;
    private processes: string;
    private workers: string;
    private expiringWorkers: string;
    private permanentWorkers: string;
    private workerTimers: string;
    private workingQueues: string;
    private workingProcesses: string;

    /**
     * Creates a Worqr instance.
     */
    public constructor(redisOptions: RedisOptions, worqrOptions?: WorqrOptions) {
        super();
        this.pub = redisOptions.data || redis.createClient(redisOptions);
        this.sub = redisOptions.subscribe || redis.createClient(redisOptions);
        this.redisKeyPrefix = (worqrOptions && worqrOptions.redisKeyPrefix) || this.redisKeyPrefix;
        this.workerId = (worqrOptions && worqrOptions.instanceId) || this.workerId;
        this.workerHeartbeatInterval = (worqrOptions && worqrOptions.workerHeartbeatInterval) || this.workerHeartbeatInterval;
        this.workerTimeout = (worqrOptions && worqrOptions.workerTimeout) || this.workerTimeout;
        this.workerCleanupInterval = (worqrOptions && worqrOptions.workerCleanupInterval) || this.workerCleanupInterval;
        this.digestBiteSize = (worqrOptions && worqrOptions.digestBiteSize) || this.digestBiteSize;
        this.queues = `${this.redisKeyPrefix}:queues`;
        this.processes = `${this.redisKeyPrefix}:processes`;
        this.workers = `${this.redisKeyPrefix}:workers`;
        this.expiringWorkers = `${this.redisKeyPrefix}:expiringWorkers`;
        this.permanentWorkers = `${this.redisKeyPrefix}:permanentWorkers`;
        this.workerTimers = `${this.redisKeyPrefix}:workerTimers`;
        this.workingQueues = `${this.redisKeyPrefix}:workingQueues`;
        this.workingProcesses = `${this.redisKeyPrefix}:workingProcesses`;

        this.sub.on('message', (channel, message) => {
            const unprefixedChannel = channel.substr(channel.indexOf('_') + 1);
            const lastUnderscore = unprefixedChannel.lastIndexOf('_');
            const queueName = unprefixedChannel.substr(0, lastUnderscore);
            const type = unprefixedChannel.substr(lastUnderscore + 1);

            this.emit(queueName, type, message);
        });

        setInterval(() => {
            this.cleanupWorkers();
        }, this.workerCleanupInterval);
    }

    // #region Queues

    /**
     * Returns a list of all queues.
     */
    public getQueues(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.keys(`${this.queues}*`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames.map(queueName => queueName.split(':')[2]));
            });
        });
    }

    /**
     * Returns a list of queues a worker is working on.
     */
    private getWorkingQueues(workerId: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(`${this.workingQueues}:${workerId}`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames);
            });
        });
    }

    /**
     * Returns the next task in a queue.
     */
    public peekQueue(queueName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.pub.lindex(`${this.queues}:${queueName}`, -1, (err, task) => {
                if (err) return reject(err);
                resolve(task);
            });
        });
    }

    /**
     * Returns the number of tasks in a queue.
     */
    public countQueue(queueName: string): Promise<number> {
        return new Promise((resolve, reject) => {
            this.pub.llen(`${this.queues}:${queueName}`, (err, len) => {
                if (err) return reject(err);
                resolve(len);
            });
        });
    }

    /**
     * Deletes a queue, emitting an event with `type: 'delete'`.
     * Clients should listen for this event and stop work on the queue.
     */
    public deleteQueue(queueName: string): Promise<void> {
        log(`deleting ${queueName}`);

        return new Promise((resolve, reject) => {
            this.pub.multi()
                .del(`${this.queues}:${queueName}`)
                .publish(`${this.redisKeyPrefix}_${queueName}_delete`, '')
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Tasks

    /**
     * Enqueues a task on a queue, emitting an event with `type: 'work'`.
     * Clients should listen for this event and start tasks on the queue.
     */
    public enqueue(queueName: string, task: string | string[]): Promise<void> {
        log(`queueing ${task.toString()} to ${queueName}`);

        return new Promise((resolve, reject) => {
            this.pub.multi()
                .lpush(`${this.queues}:${queueName}`, task)
                .publish(`${this.redisKeyPrefix}_${queueName}_work`, Array.isArray(task) ? task.length.toString() : '1')
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    /**
     * Returns all the tasks in a queue.
     */
    public getTasks(queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.lrange(`${this.queues}:${queueName}`, 0, -1, (err, tasks) => {
                if (err) return reject(err);
                resolve(tasks);
            });
        });
    }

    /**
     * Returns the task for a given process.
     */
    private getTaskForProcess(processId: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.pub.lindex(`${this.processes}:${processId}`, 0, (err, task) => {
                if (err) return reject(err);
                resolve(task);
            });
        });
    }

    /**
     * Removes a task from the queue, emitting an event with `type: 'cancel'` and  `message: <task>`.
     * Clients should listen for this event and stop all processes matching the task.
     */
    public cancelTasks(queueName: string, task: string): Promise<void> {
        log(`canceling ${task} on ${queueName}`);

        return new Promise((resolve, reject) => {
            this.pub.multi()
                .lrem(`${this.queues}:${queueName}`, 0, task)
                .publish(`${this.redisKeyPrefix}_${queueName}_cancel`, task)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Processes

    /**
     * Dequeues a task from the queue, returning a process.
     * The worker must have started work on the queue in order to get tasks.
     * Process will be null if the queue is empty.
     */
    public dequeue(queueName: string): Promise<Process | null> {
        log(`${this.workerId} starting task on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.isWorking(this.workerId, queueName))
                .then(isWorking => new Promise<Process>((resolve, reject) => {
                    if (!isWorking) return reject(`${this.workerId} is not working on ${queueName}`);

                    const processId = `${queueName}_${uuid.v4()}`;

                    this.pub.multi()
                        .rpoplpush(`${this.queues}:${queueName}`, `${this.processes}:${processId}`)
                        .sadd(`${this.workingProcesses}:${queueName}`, processId)
                        .exec((err, [task]) => {
                            if (err) return reject(err);
                            resolve({ id: processId, task });
                        });
                }))
                .then(process => {
                    if (!process.task) {
                        this.pub.srem(`${this.workingProcesses}:${queueName}`, process.id, err => {
                            if (err) return reject(err);
                            resolve(null);
                        });
                    } else {
                        resolve(process);
                    }
                })
                .catch(err => reject(err));
        });
    }

    /**
     * Returns a list of all processes running.
     */
    private getProcesses(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.keys(`${this.processes}*`, (err, processIds) => {
                if (err) return reject(err);
                resolve(processIds.map(processId => processId.split(':')[2]));
            });
        });
    }

    /**
     * Returns a list of processes for tasks matching the given task.
     */
    public getMatchingProcesses(task: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            const processIdsForTask: string[] = [];

            Promise.resolve()
                .then(() => this.getProcesses())
                .then(processIds => Promise.all(processIds.map(processId => this.getTaskForProcess(processId).then(t => {
                    if (t === task) {
                        processIdsForTask.push(processId);
                    }
                }))))
                .then(() => resolve(processIdsForTask))
                .catch(err => reject(err));
        });
    }

    /**
     * Returns a list of all processes running on a queue.
     */
    private getWorkingProcesses(queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(`${this.workingProcesses}:${queueName}`, (err, processIds) => {
                if (err) return reject(err);
                resolve(processIds);
            });
        });
    }

    /**
     * Stops a process, returning its task to the queue it came from.
     */
    public stopProcess(processId: string): Promise<void> {
        log(`stopping process ${processId}`);

        return new Promise((resolve, reject) => {
            const queueName = processId.substr(0, processId.lastIndexOf('_'));

            this.pub.multi()
                .rpoplpush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
                .srem(`${this.workingProcesses}:${queueName}`, processId)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    /**
     * Stops a process, removing the task entirely.
     */
    public finishProcess(processId: string): Promise<void> {
        log(`finishing process ${processId}`);

        return new Promise((resolve, reject) => {
            const queueName = processId.substr(0, processId.lastIndexOf('_'));

            this.pub.multi()
                .del(`${this.processes}:${processId}`)
                .srem(`${this.workingProcesses}:${queueName}`, processId)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Worker

    /**
     * Returns the unique worker ID for this instance.
     */
    public getWorkerId(): string {
        return this.workerId;
    }

    /**
     * Starts this worker.
     */
    public startWorker(): Promise<void> {
        log(`starting worker ${this.workerId}`);

        return new Promise((resolve, reject) => {
            const multi = this.pub.multi()
                .sadd(this.workers, this.workerId);

            if (this.workerTimeout < 0) {
                multi.sadd(this.permanentWorkers, this.workerId);
            } else {
                multi
                    .sadd(this.expiringWorkers, this.workerId)
                    .set(`${this.workerTimers}:${this.workerId}`, 'RUN', 'EX', this.workerTimeout);
            }

            multi.exec(err => {
                if (err) return reject(err);

                this.workerTimerInterval = setInterval(() => {
                    this.keepWorkerAlive();
                }, this.workerHeartbeatInterval);

                resolve();
            });
        });
    }

    /**
     * Starts work on a queue.
     * The worker will start emitting events for the queue, which clients should subscribe to.
     * This also emits an event immediately if there are tasks on the queue.
     */
    public startWork(queueName: string): Promise<void> {
        log(`${this.workerId} starting work on ${queueName}`);

        return new Promise((resolve, reject) => {
            this.pub.multi()
                .sadd(`${this.workingQueues}:${this.workerId}`, queueName)
                .exec(err => {
                    if (err) return reject(err);

                    this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                    this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);
                    this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_delete`);

                    this.requestWork(queueName);

                    resolve();
                });
        });
    }

    /**
     * Requests a task from the queue.
     * If there is one, an event with `type: 'work'` will be published.
     * This is so the client doesn't have to set up their own polling of the queue.
     */
    public requestWork(queueName: string): void {
        this.peekQueue(queueName).then(task => {
            if (task) {
                this.pub.publish(`${this.redisKeyPrefix}_${queueName}_work`, '1');
            }
        });
    }

    /**
     * Stops work on a queue.
     * The worker will stop emitting events for the queue.
     */
    public stopWork(queueName: string): Promise<void> {
        log(`${this.workerId} stopping work on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingProcesses(queueName))
                .then(processIds => {
                    let multi = this.pub.multi()
                        .srem(`${this.workingQueues}:${this.workerId}`, queueName)
                        .del(`${this.workingProcesses}:${queueName}`);

                    processIds.forEach(processId => {
                        multi = multi
                            .rpoplpush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processId}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                        this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);
                        this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_delete`);

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    /**
     * Refreshes this worker's timer, indicating it is still active.
     */
    private keepWorkerAlive(): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.workerTimeout <= 0)
                return resolve();

            this.pub.set(`${this.workerTimers}:${this.workerId}`, 'RUN', 'EX', this.workerTimeout, 'XX', (err, success) => {
                if (err) return reject(err);
                if (!success) return this.failWorker(this.workerId);
                resolve();
            });
        });
    }

    // #endregion

    // #region Workers

    /**
     * Returns a list of all workers.
     */
    public getWorkers(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(this.workers, (err, workerIds) => {
                if (err) return reject(err);
                resolve(workerIds);
            });
        });
    }

    /**
     * Returns whether a worker is working on a queue.
     */
    public isWorking(workerId: string, queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.pub.sismember(`${this.workingQueues}:${workerId}`, queueName, (err, isMember) => {
                if (err) return reject(err);
                resolve(isMember === 1);
            });
        });
    }

    /**
     * Fails a worker, putting all its tasks back on the queues they came from.
     */
    public failWorker(workerId?: string): Promise<void> {
        if (!workerId) workerId = this.workerId;

        log(`failing ${workerId}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingQueues(workerId as string))
                .then(queueNames => new Promise<WorkerItems>((resolve, reject) => {
                    const processIds: string[] = [];

                    Promise.all(queueNames.map(queueName => this.getWorkingProcesses(queueName)))
                        .then(processIds2d => processIds2d.forEach(processIds1d => processIds.push(...processIds1d)))
                        .then(() => resolve({ queueNames, processIds }))
                        .catch(err => reject(err));
                }))
                .then(({ queueNames, processIds }) => {
                    let multi = this.pub.multi()
                        .srem(this.workers, workerId as string)
                        .srem(this.expiringWorkers, workerId as string)
                        .srem(this.permanentWorkers, workerId as string)
                        .del(`${this.workerTimers}:${workerId}`)
                        .del(`${this.workingQueues}:${workerId}`);

                    queueNames.forEach(queueName => {
                        multi = multi
                            .del(`${this.workingProcesses}:${queueName}`);
                    });

                    processIds.forEach(processId => {
                        const queueName = processId.substr(0, processId.lastIndexOf('_'));

                        multi = multi
                            .rpoplpush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processId}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        if (workerId === this.workerId) {
                            queueNames.forEach(queueName => {
                                this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                                this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);
                                this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_delete`);
                            });

                            if (this.workerTimerInterval) {
                                clearInterval(this.workerTimerInterval);
                            }
                        }

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    /**
     * Returns a list of all expiring workers.
     */
    private getExpiringWorkers(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(this.expiringWorkers, (err, workerIds) => {
                if (err) return reject(err);
                resolve(workerIds);
            });
        });
    }

    /**
     * Fails all workers that have timed out.
     * This is run every `workerCleanupInterval` milliseconds.
     */
    public cleanupWorkers(): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getExpiringWorkers())
                .then(workerIds => Promise.all(workerIds.map(workerId => new Promise<WorkerStatus>((resolve, reject) => {
                    this.pub.keys(`${this.workerTimers}:${workerId}`, (err, workerTimers) => {
                        if (err) return reject(err);
                        resolve({ workerId, dead: workerTimers.length === 0 });
                    });
                }))))
                .then(results => Promise.all(results
                    .filter(({ workerId, dead }) => workerId !== this.workerId && dead)
                    .map(({ workerId }) => this.failWorker(workerId))))
                .then(() => resolve())
                .catch(err => reject(err));
        });
    }

    // #endregion
}
