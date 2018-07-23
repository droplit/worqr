import { EventEmitter } from 'events';
import * as redis from 'redis';
import * as uuid from 'uuid';

const log = require('debug')('worqr');

export class Worqr extends EventEmitter {
    private publisher: redis.RedisClient;
    private subscriber: redis.RedisClient;
    private redisKeyPrefix = 'worqr';
    private workerId = uuid.v4();
    private workerHeartbeatInterval = 1000;
    private workerTimeout = 3;
    private digestBiteSize = 0;
    private digestInterval: NodeJS.Timer | null = null;
    private queues: string;
    private processes: string;
    private workers: string;
    private workerTimers: string;
    private workingQueues: string;
    private workingProcesses: string;

    public constructor(redisOptions: { host: string, port: number, options?: redis.ClientOpts }, worqrOptions?: { redisKeyPrefix?: string, instanceId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, digestBiteSize?: number }) {
        super();
        this.publisher = redis.createClient(redisOptions);
        this.subscriber = redis.createClient(redisOptions);
        this.redisKeyPrefix = (worqrOptions && worqrOptions.redisKeyPrefix) || this.redisKeyPrefix;
        this.workerId = (worqrOptions && worqrOptions.instanceId) || this.workerId;
        this.workerHeartbeatInterval = (worqrOptions && worqrOptions.workerHeartbeatInterval) || this.workerHeartbeatInterval;
        this.workerTimeout = (worqrOptions && worqrOptions.workerTimeout) || this.workerTimeout;
        this.digestBiteSize = (worqrOptions && worqrOptions.digestBiteSize) || this.digestBiteSize;
        this.queues = `${this.redisKeyPrefix}:queues`;
        this.processes = `${this.redisKeyPrefix}:processes`;
        this.workers = `${this.redisKeyPrefix}:workers`;
        this.workerTimers = `${this.redisKeyPrefix}:workerTimers`;
        this.workingQueues = `${this.redisKeyPrefix}:workingQueues`;
        this.workingProcesses = `${this.redisKeyPrefix}:workingProcesses`;

        this.subscriber.on('message', (channel, message) => {
            const unprefixedChannel = channel.substr(channel.indexOf('_') + 1);
            const lastUnderscore = unprefixedChannel.lastIndexOf('_');
            const queueName = unprefixedChannel.substr(0, lastUnderscore);
            const type = unprefixedChannel.substr(lastUnderscore + 1);

            this.emit(queueName, { type, message });
        });
    }

    public getWorkerId(): string {
        return this.workerId;
    }

    // #region Queues

    public getQueueNames(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.keys(`${this.queues}*`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames.map(queueName => queueName.split(':')[2]));
            });
        });
    }

    public enqueue(queueName: string, task: string | string[]): Promise<void> {
        log(`queueing ${task.toString()} to ${queueName}`);

        return new Promise((resolve, reject) => {
            this.publisher.multi()
                .lpush(`${this.queues}:${queueName}`, task, err => {
                    if (err) return reject(err);
                    resolve();
                })
                .publish(`${this.redisKeyPrefix}_${queueName}_work`, task.toString())
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public isQueue(queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.publisher.exists(`${this.queues}:${queueName}`, (err, exists) => {
                if (err) return reject(err);
                resolve(exists === 1);
            });
        });
    }

    public deleteQueue(queueName: string): Promise<void> {
        log(`deleting ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingProcesses(queueName))
                .then(processNames => {
                    let multi = this.publisher.multi()
                        .del(`${this.queues}:${queueName}`)
                        .srem(`${this.workingQueues}:${this.workerId}`, queueName)
                        .del(`${this.workingProcesses}:${queueName}`);

                    processNames.forEach(processName => {
                        multi = multi
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        this.subscriber.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                        this.subscriber.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    // #endregion

    // #region Tasks

    public startTask(queueName: string): Promise<[string | null, string | null]> {
        log(`${this.workerId} starting task on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.isWorking(queueName))
                .then(isWorking => new Promise<[string, string]>((resolve, reject) => {
                    if (!isWorking) return reject(`${this.workerId} is not working on ${queueName}`);

                    const processName = `${queueName}_${uuid.v4()}`;

                    this.publisher.multi()
                        .rpoplpush(`${this.queues}:${queueName}`, `${this.processes}:${processName}`)
                        .sadd(`${this.workingProcesses}:${queueName}`, processName)
                        .exec((err, [task]) => {
                            if (err) return reject(err);
                            resolve([processName, task]);
                        });
                }))
                .then(([processName, task]) => {
                    if (!task) {
                        this.publisher.srem(`${this.workingProcesses}:${queueName}`, processName, err => {
                            if (err) return reject(err);
                            resolve([null, null]);
                        });
                    } else {
                        resolve([processName, task]);
                    }
                })
                .catch(err => reject(err));
        });
    }

    public getProcesses(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.keys(`${this.processes}*`, (err, processNames) => {
                if (err) return reject(err);
                resolve(processNames.map(processName => processName.split(':')[2]));
            });
        });
    }

    public getTask(processName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.publisher.lindex(`${this.processes}:${processName}`, 0, (err, task) => {
                if (err) return reject(err);
                resolve(task);
            });
        });
    }

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

    public stopTask(processName: string): Promise<void> {
        log(`stopping process ${processName}`);

        return new Promise((resolve, reject) => {
            const queueName = processName.split('_')[0];

            this.publisher.multi()
                .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                .srem(`${this.workingProcesses}:${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public finishTask(processName: string): Promise<void> {
        log(`finishing process ${processName}`);

        return new Promise((resolve, reject) => {
            const queueName = processName.split('_')[0];

            this.publisher.multi()
                .del(`${this.processes}:${processName}`)
                .srem(`${this.workingProcesses}:${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public cancelTasks(queueName: string, task: string): Promise<void> {
        log(`canceling tasks ${task}`);

        return new Promise((resolve, reject) => {
            this.publisher.multi()
                .lrem(`${this.queues}:${queueName}`, 0, task)
                .publish(`${this.redisKeyPrefix}_${queueName}_cancel`, task)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Workers

    public startWorker(): Promise<void> {
        log(`starting worker ${this.workerId}`);

        return new Promise((resolve, reject) => {
            this.publisher.multi()
                .sadd(this.workers, this.workerId)
                .set(`${this.workerTimers}:${this.workerId}`, 'RUN', 'EX', this.workerTimeout)
                .exec(err => {
                    if (err) return reject(err);

                    this.digestInterval = setInterval(() => {
                        this.keepWorkerAlive();
                    }, this.workerHeartbeatInterval);

                    resolve();
                });
        });
    }

    public keepWorkerAlive(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.publisher.set(`${this.workerTimers}:${this.workerId}`, 'RUN', 'EX', this.workerTimeout, 'XX', (err, success) => {
                if (err) return reject(err);
                if (!success) return this.failWorker();
                resolve();
            });
        });
    }

    public startWork(queueName: string): Promise<void> {
        log(`${this.workerId} starting work on ${queueName}`);

        return new Promise((resolve, reject) => {
            this.publisher.multi()
                .sadd(`${this.workingQueues}:${this.workerId}`, queueName)
                .exec(err => {
                    if (err) return reject(err);

                    this.subscriber.subscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                    this.subscriber.subscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);

                    resolve();
                });
        });
    }

    public isWorking(queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.publisher.sismember(`${this.workingQueues}:${this.workerId}`, queueName, (err, isMember) => {
                if (err) return reject(err);
                resolve(isMember === 1);
            });
        });
    }

    public getWorkingQueues(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.smembers(`${this.workingQueues}:${this.workerId}`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames);
            });
        });
    }

    public getWorkingProcesses(queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.smembers(`${this.workingProcesses}:${queueName}`, (err, processNames) => {
                if (err) return reject(err);
                resolve(processNames);
            });
        });
    }

    public stopWork(queueName: string): Promise<void> {
        log(`${this.workerId} stopping work on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingProcesses(queueName))
                .then(processNames => {
                    let multi = this.publisher.multi()
                        .srem(`${this.workingQueues}:${this.workerId}`, queueName)
                        .del(`${this.workingProcesses}:${queueName}`);

                    processNames.forEach(processName => {
                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        this.subscriber.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                        this.subscriber.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    public failWorker(): Promise<void> {
        log(`failing ${this.workerId}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingQueues())
                .then(queueNames => new Promise<[string[], string[]]>((resolve, reject) => {
                    const processNames: string[] = [];

                    Promise.all(queueNames.map(queueName => this.getWorkingProcesses(queueName)))
                        .then(processNames2d => processNames2d.forEach(processNames1d => processNames.push(...processNames1d)))
                        .then(() => resolve([queueNames, processNames]))
                        .catch(err => reject(err));
                }))
                .then(([queueNames, processNames]) => {
                    let multi = this.publisher.multi()
                        .srem(this.workers, this.workerId)
                        .del(`${this.workerTimers}:${this.workerId}`)
                        .del(`${this.workingQueues}:${this.workerId}`);

                    queueNames.forEach(queueName => {
                        multi = multi
                            .del(`${this.workingProcesses}:${queueName}`);
                    });

                    processNames.forEach(processName => {
                        const queueName = processName.split('_')[0];

                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        queueNames.forEach(queueName => {
                            this.subscriber.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                            this.subscriber.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);
                        });

                        clearInterval(this.digestInterval as NodeJS.Timer);

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    // #endregion
}
