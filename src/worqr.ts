import { EventEmitter } from 'events';
import * as redis from 'redis';
import * as uuid from 'uuid';

const log = require('debug')('worqr');

/**
 * Represents a process started by a worker.
 */
export interface Process {
    processName: string;
    task: string;
}

/**
 * Event emitted by a queue.
 */
export interface QueueEvent {
    type: 'work' | 'cancel' | 'delete';
    message: string;
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
    public constructor(redisOptions: { host: string, port: number, password: string }, worqrOptions?: { redisKeyPrefix?: string, instanceId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, workerCleanupInterval?: number, digestBiteSize?: number }) {
        super();
        this.pub = redis.createClient(redisOptions);
        this.sub = redis.createClient(redisOptions);
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

            this.emit(queueName, { type, message });
        });

        setInterval(() => {
            this.cleanupWorkers();
        }, this.workerCleanupInterval);
    }

    // #region Queues

    /**
     * Returns a list of all queues.
     */
    public getQueueNames(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.keys(`${this.queues}*`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames.map(queueName => queueName.split(':')[2]));
            });
        });
    }

    /**
     * Enqueues a task on a queue, emitting an event with `type: 'work'`.
     * Clients should listen for this event and start tasks on the queue.
     */
    public enqueue(queueName: string, task: string | string[]): Promise<void> {
        log(`queueing ${task.toString()} to ${queueName}`);

        return new Promise((resolve, reject) => {
            this.pub.multi()
                .lpush(`${this.queues}:${queueName}`, task)
                .publish(`${this.redisKeyPrefix}_${queueName}_work`, '1')
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    /**
     * Returns whether there is a queue with this name.
     */
    public isQueue(queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.pub.exists(`${this.queues}:${queueName}`, (err, exists) => {
                if (err) return reject(err);
                resolve(exists === 1);
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
    public getQueueCount(queueName: string): Promise<number> {
        return new Promise((resolve, reject) => {
            this.pub.llen(`${this.queues}:${queueName}`, (err, len) => {
                if (err) return reject(err);
                resolve(len);
            });
        });
    }

    /**
     * Returns all the tasks in a queue.
     */
    public getQueueTasks(queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.lrange(`${this.queues}:${queueName}`, 0, -1, (err, tasks) => {
                if (err) return reject(err);
                resolve(tasks);
            });
        });
    }

    /**
     * Returns a list of all processes running on a queue.
     */
    public getWorkingProcesses(queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(`${this.workingProcesses}:${queueName}`, (err, processNames) => {
                if (err) return reject(err);
                resolve(processNames);
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
                .publish(`${this.redisKeyPrefix}_${queueName}_delete`, '1')
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Tasks

    /**
     * Dequeues a task from the queue, returning a process.
     * The worker must have started work on the queue in order to get tasks.
     * Process will be null if the queue is empty.
     */
    public startTask(queueName: string): Promise<Process | null> {
        log(`${this.workerId} starting task on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.isWorking(this.workerId, queueName))
                .then(isWorking => new Promise<Process>((resolve, reject) => {
                    if (!isWorking) return reject(`${this.workerId} is not working on ${queueName}`);

                    const processName = `${queueName}_${uuid.v4()}`;

                    this.pub.multi()
                        .rpoplpush(`${this.queues}:${queueName}`, `${this.processes}:${processName}`)
                        .sadd(`${this.workingProcesses}:${queueName}`, processName)
                        .exec((err, [task]) => {
                            if (err) return reject(err);
                            resolve({ processName, task });
                        });
                }))
                .then(process => {
                    const { processName, task } = process;

                    if (!task) {
                        this.pub.srem(`${this.workingProcesses}:${queueName}`, processName, err => {
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
    public getProcesses(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.keys(`${this.processes}*`, (err, processNames) => {
                if (err) return reject(err);
                resolve(processNames.map(processName => processName.split(':')[2]));
            });
        });
    }

    /**
     * Returns the task for a given process.
     */
    public getTask(processName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.pub.lindex(`${this.processes}:${processName}`, 0, (err, task) => {
                if (err) return reject(err);
                resolve(task);
            });
        });
    }

    /**
     * Returns a list of processes for tasks matching the given task.
     */
    public getMatchingProcesses(task: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            const processNamesForTask: string[] = [];

            Promise.resolve()
                .then(() => this.getProcesses())
                .then(processNames => Promise.all(processNames.map(processName => this.getTask(processName).then(t => {
                    if (t === task) {
                        processNamesForTask.push(processName);
                    }
                }))))
                .then(() => resolve(processNamesForTask))
                .catch(err => reject(err));
        });
    }

    /**
     * Stops a process, returning its task to the queue it came from.
     */
    public stopProcess(processName: string): Promise<void> {
        log(`stopping process ${processName}`);

        return new Promise((resolve, reject) => {
            const queueName = processName.substr(0, processName.lastIndexOf('_'));

            this.pub.multi()
                .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                .srem(`${this.workingProcesses}:${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    /**
     * Stops a process, removing the task entirely.
     */
    public finishProcess(processName: string): Promise<void> {
        log(`finishing process ${processName}`);

        return new Promise((resolve, reject) => {
            const queueName = processName.substr(0, processName.lastIndexOf('_'));

            this.pub.multi()
                .del(`${this.processes}:${processName}`)
                .srem(`${this.workingProcesses}:${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    /**
     * Removes a task from the queue, emitting an event with `type: 'cancel', message: <task>`.
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
     * Refreshes this worker's timer, indicating it is still active.
     */
    public keepWorkerAlive(): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.workerTimeout < 0)
                return resolve();

            this.pub.set(`${this.workerTimers}:${this.workerId}`, 'RUN', 'EX', this.workerTimeout, 'XX', (err, success) => {
                if (err) return reject(err);
                if (!success) return this.failWorker(this.workerId);
                resolve();
            });
        });
    }

    /**
     * Starts work on a queue.
     * The worker will start emitting events for the queue, which clients should subscribe to.
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

                    this.isQueue(queueName).then(isQueue => {
                        if (isQueue) {
                            this.pub.publish(`${this.redisKeyPrefix}_${queueName}_work`, '1');
                        }
                    });

                    resolve();
                });
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
                .then(processNames => {
                    let multi = this.pub.multi()
                        .srem(`${this.workingQueues}:${this.workerId}`, queueName)
                        .del(`${this.workingProcesses}:${queueName}`);

                    processNames.forEach(processName => {
                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
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

    // #endregion

    // #region Workers

    /**
     * Returns a list of all workers.
     */
    public getWorkers(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(this.workers, (err, workerNames) => {
                if (err) return reject(err);
                resolve(workerNames);
            });
        });
    }

    /**
     * Returns a list of all expiring workers.
     */
    public getExpiringWorkers(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(this.expiringWorkers, (err, workerNames) => {
                if (err) return reject(err);
                resolve(workerNames);
            });
        });
    }

    /**
     * Returns a list of all permanent workers.
     */
    public getPermanentWorkers(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(this.permanentWorkers, (err, workerNames) => {
                if (err) return reject(err);
                resolve(workerNames);
            });
        });
    }

    /**
     * Returns whether a worker is working on a queue.
     */
    public isWorking(workerName: string, queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.pub.sismember(`${this.workingQueues}:${workerName}`, queueName, (err, isMember) => {
                if (err) return reject(err);
                resolve(isMember === 1);
            });
        });
    }

    /**
     * Returns a list of all queues a worker is working on.
     */
    public getWorkingQueues(workerName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.pub.smembers(`${this.workingQueues}:${workerName}`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames);
            });
        });
    }

    /**
     * Fails a worker, putting all its tasks back on the queues they came from.
     */
    public failWorker(workerName?: string): Promise<void> {
        if (!workerName) workerName = this.workerId;

        log(`failing ${workerName}`);

        return new Promise((resolve, reject) => {
            /**
             * Represents queues and processes that this worker is working on.
             */
            interface WorkerItems {
                queueNames: string[];
                processNames: string[];
            }

            Promise.resolve()
                .then(() => this.getWorkingQueues(workerName as string))
                .then(queueNames => new Promise<WorkerItems>((resolve, reject) => {
                    const processNames: string[] = [];

                    Promise.all(queueNames.map(queueName => this.getWorkingProcesses(queueName)))
                        .then(processNames2d => processNames2d.forEach(processNames1d => processNames.push(...processNames1d)))
                        .then(() => resolve({ queueNames, processNames }))
                        .catch(err => reject(err));
                }))
                .then(({ queueNames, processNames }) => {
                    let multi = this.pub.multi()
                        .srem(this.workers, workerName as string)
                        .srem(this.expiringWorkers, workerName as string)
                        .srem(this.permanentWorkers, workerName as string)
                        .del(`${this.workerTimers}:${workerName}`)
                        .del(`${this.workingQueues}:${workerName}`);

                    queueNames.forEach(queueName => {
                        multi = multi
                            .del(`${this.workingProcesses}:${queueName}`);
                    });

                    processNames.forEach(processName => {
                        const queueName = processName.substr(0, processName.lastIndexOf('_'));

                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        if (workerName === this.workerId) {
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
     * Fails all workers that have timed out.
     * This is run every `workerCleanupInterval` milliseconds.
     */
    public cleanupWorkers(): Promise<void> {
        return new Promise((resolve, reject) => {
            /**
             * Represents whether a worker is dead.
             */
            interface WorkerStatus {
                workerName: string;
                dead: boolean;
            }

            Promise.resolve()
                .then(() => this.getExpiringWorkers())
                .then(workerNames => Promise.all(workerNames.map(workerName => new Promise<WorkerStatus>((resolve, reject) => {
                    this.pub.keys(`${this.workerTimers}:${workerName}`, (err, workerTimers) => {
                        if (err) return reject(err);
                        resolve({ workerName, dead: workerTimers.length === 0 });
                    });
                }))))
                .then(results => Promise.all(results
                    .filter(({ workerName, dead }) => workerName !== this.workerId && dead)
                    .map(({ workerName }) => this.failWorker(workerName))))
                .then(() => resolve())
                .catch(err => reject(err));
        });
    }

    // #endregion
}
