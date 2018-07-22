import { EventEmitter } from 'events';
import * as redis from 'redis';
import * as uuid from 'uuid';
import { resolve } from 'path';

const log = require('debug')('worqr');

export class Worqr extends EventEmitter {
    private publisher: redis.RedisClient;
    private subscriber: redis.RedisClient;
    private redisKeyPrefix = 'worqr';
    private workerId = uuid.v4();
    private workerHeartbeatInterval = 1;
    private workerTimeout = 3;
    private digestBiteSize = 0;
    private queues: string;
    private processes: string;
    private workers: string;
    private workerTimers: string;
    private workingQueues: string;
    private workingProcesses: string;

    public constructor(redisOptions: { host: string, port: number, options?: redis.ClientOpts }, worqrOptions?: { redisKeyPrefix?: string, workerId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, digestBiteSize?: number }) {
        super();
        this.publisher = redis.createClient(redisOptions);
        this.subscriber = redis.createClient(redisOptions);
        this.redisKeyPrefix = (worqrOptions && worqrOptions.redisKeyPrefix) || this.redisKeyPrefix;
        this.workerId = (worqrOptions && worqrOptions.workerId) || this.workerId;
        this.workerHeartbeatInterval = (worqrOptions && worqrOptions.workerHeartbeatInterval) || this.workerHeartbeatInterval;
        this.workerTimeout = (worqrOptions && worqrOptions.workerTimeout) || this.workerTimeout;
        this.digestBiteSize = (worqrOptions && worqrOptions.digestBiteSize) || this.digestBiteSize;
        this.queues = `${this.redisKeyPrefix}:queues`;
        this.processes = `${this.redisKeyPrefix}:processes`;
        this.workers = `${this.redisKeyPrefix}:workers:${this.workerId}`;
        this.workerTimers = `${this.redisKeyPrefix}:workerTimers:${this.workerId}`;
        this.workingQueues = `${this.redisKeyPrefix}:workingQueues:${this.workerId}`;
        this.workingProcesses = `${this.redisKeyPrefix}:workingProcesses:${this.workerId}`;

        this.subscriber.on('message', (channel, message) => {
            const queueName = channel.split('_')[0];
            const type = channel.split('_')[1];

            this.emit(queueName, { type, message });
        });

        // digest
        setInterval(() => {
            this.publisher.smembers(this.workers, (err, workerNames) => {
                if (err) return log(err);

                workerNames.forEach(workerName => {
                    this.keepWorkerAlive(workerName);
                });
            });
        }, this.workerHeartbeatInterval);
    }

    // #region Queues

    public getQueueNames(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.keys(`${this.queues}*`, (err, queues) => {
                if (err) return reject(err);
                resolve(queues);
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
                .publish(`${queueName}_work`, task.toString())
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
            this.publisher.multi()
                .del(`${this.queues}:${queueName}`)
                // TODO: need to do a lot of other stuff
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Tasks

    public startTask(queueName: string, workerName: string): Promise<[string, string]> {
        log(`${workerName} starting task on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.isWorking(workerName, queueName))
                .then(isWorking => {
                    if (!isWorking) return reject(`${workerName} is not working on ${queueName}`);

                    const processName = `${workerName}_${queueName}_${uuid.v4()}`;

                    this.publisher.multi()
                        .rpoplpush(`${this.queues}:${queueName}`, `${this.processes}:${processName}`)
                        .sadd(`${this.workingProcesses}:${workerName}_${queueName}`, processName)
                        .exec((err, [task]) => {
                            if (err) return reject(err);
                            resolve([processName, task]);
                        });
                });
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

    public getProcessesForTask(task: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            const processNamesForTask: string[] = [];

            Promise.resolve()
                .then(() => this.getProcesses())
                .then(processNames => Promise.all(processNames.map(processName => new Promise((resolve, reject) => {
                    console.log(processName);
                    this.publisher.lindex(`${this.processes}:${processName}`, 0, (err, t) => {
                        console.log(t);
                        if (err) return reject(err);

                        if (t === task) {
                            processNamesForTask.push(processName);
                        }

                        resolve();
                    });
                }))))
                .then(() => resolve(processNamesForTask))
                .catch(err => reject(err));
        });
    }

    public stopTask(processName: string): Promise<void> {
        log(`stopping process ${processName}`);

        return new Promise((resolve, reject) => {
            const workerName = processName.split('_')[0];
            const queueName = processName.split('_')[1];

            this.publisher.multi()
                .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                .srem(`${this.workingProcesses}:${workerName}_${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public finishTask(processName: string): Promise<void> {
        log(`finishing process ${processName}`);

        return new Promise((resolve, reject) => {
            const workerName = processName.split('_')[0];
            const queueName = processName.split('_')[1];

            this.publisher.multi()
                .del(`${this.processes}:${processName}`)
                .srem(`${this.workingProcesses}:${workerName}_${queueName}`, processName)
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
                .publish(`${queueName}_cancel`, task)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Workers

    public startWorker(workerName: string): Promise<void> {
        log(`starting worker ${workerName}`);

        return new Promise((resolve, reject) => {
            this.publisher.multi()
                .sadd(this.workers, workerName)
                .set(`${this.workerTimers}:${workerName}`, 'RUN', 'EX', this.workerTimeout)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public keepWorkerAlive(workerName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.publisher.set(`${this.workerTimers}:${workerName}`, 'RUN', 'EX', this.workerTimeout, 'XX', (err, success) => {
                if (err) return reject(err);
                if (!success) return this.failWorker(workerName);
                resolve();
            });
        });
    }

    public startWork(workerName: string, queueName: string): Promise<void> {
        log(`${workerName} starting work on ${queueName}`);

        return new Promise((resolve, reject) => {
            this.publisher.multi()
                .sadd(`${this.workingQueues}:${workerName}`, queueName)
                .exec(err => {
                    if (err) return reject(err);

                    this.subscriber.subscribe(`${queueName}_work`);

                    resolve();
                });
        });
    }

    public isWorking(workerName: string, queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.publisher.sismember(`${this.workingQueues}:${workerName}`, queueName, (err, isMember) => {
                if (err) return reject(err);
                resolve(isMember === 1);
            });
        });
    }

    public getWorkingQueues(workerName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.smembers(`${this.workingQueues}:${workerName}`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames);
            });
        });
    }

    public getWorkingProcesses(workerName: string, queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.publisher.smembers(`${this.workingProcesses}:${workerName}_${queueName}`, (err, processNames) => {
                if (err) return reject(err);
                resolve(processNames);
            });
        });
    }

    public stopWork(workerName: string, queueName: string): Promise<void> {
        log(`${workerName} stopping work on ${queueName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingProcesses(workerName, queueName))
                .then(processNames => {
                    let multi = this.publisher.multi()
                        .srem(`${this.workingQueues}:${workerName}`, queueName)
                        .del(`${this.workingProcesses}:${workerName}_${queueName}`);

                    processNames.forEach(processName => {
                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        this.subscriber.unsubscribe(queueName);

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    public failWorker(workerName: string): Promise<void> {
        log(`failing ${workerName}`);

        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingQueues(workerName))
                .then(queueNames => new Promise<[string[], string[]]>((resolve, reject) => {
                    const processNames: string[] = [];

                    Promise.all(queueNames.map(queueName => this.getWorkingProcesses(workerName, queueName)))
                        .then(processNames2d => processNames2d.forEach(processNames1d => processNames.push(...processNames1d)))
                        .then(() => resolve([queueNames, processNames]))
                        .catch(err => reject(err));
                }))
                .then(([queueNames, processNames]) => {
                    let multi = this.publisher.multi()
                        .srem(this.workers, workerName)
                        .del(`${this.workerTimers}:${workerName}`)
                        .del(`${this.workingQueues}:${workerName}`);

                    queueNames.forEach(queueName => {
                        multi = multi
                            .del(`${this.workingProcesses}:${workerName}_${queueName}`);
                    });

                    processNames.forEach(processName => {
                        const queueName = processName.split('_')[1];

                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        queueNames.forEach(queueName => {
                            this.subscriber.unsubscribe(queueName);
                        });

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    // #endregion
}
