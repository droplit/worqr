import * as redis from 'redis';
import { EventEmitter } from 'events';
import * as uuid from 'uuid';

const log = require('debug')('worqr');

export class Worqr extends EventEmitter {
    private redisClient: redis.RedisClient;
    private listener: redis.RedisClient;
    private redisKeyPrefix = 'worqr';
    private queues: string;
    private processes: string;
    private workers: string;
    private workerTimers: string;
    private workingQueues: string;
    private workingProcesses: string;

    public constructor(redisOptions: { host: string, port: number, options?: redis.ClientOpts }, worqrOptions?: { workerId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, redisKeyPrefix?: string, digestBiteSize?: number }) {
        super();
        this.redisClient = redis.createClient(redisOptions);
        this.listener = redis.createClient(redisOptions);
        this.redisKeyPrefix = (worqrOptions && worqrOptions.redisKeyPrefix) || this.redisKeyPrefix;
        this.queues = `${this.redisKeyPrefix}:queues`;
        this.processes = `${this.redisKeyPrefix}:processes`;
        this.workers = `${this.redisKeyPrefix}:workers`;
        this.workerTimers = `${this.redisKeyPrefix}:workerTimers`;
        this.workingQueues = `${this.redisKeyPrefix}:workingQueues`;
        this.workingProcesses = `${this.redisKeyPrefix}:workingProcesses`;

        this.listener.on('message', (channel, message) => {
            log(`${channel}: ${message}`);
        });

        // digest
        setInterval(() => {
            this.redisClient.smembers(this.workers, (err, workerNames) => {
                if (err) return log(err);

                workerNames.forEach(workerName => {
                    this.keepWorkerAlive(workerName);
                });
            });
        }, 1000);
    }

    // #region Queues

    public getQueueNames(): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.redisClient.keys(`${this.queues}*`, (err, queues) => {
                if (err) return reject(err);
                resolve(queues);
            });
        });
    }

    public enqueue(queueName: string, task: string | string[]): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
                .lpush(`${this.queues}:${queueName}`, task, err => {
                    if (err) return reject(err);
                    resolve();
                })
                .publish(`${this.queues}:${queueName}`, task.toString())
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public isQueue(queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.redisClient.exists(`${this.queues}:${queueName}`, (err, exists) => {
                if (err) return reject(err);
                resolve(exists === 1);
            });
        });
    }

    public deleteQueue(queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
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

    public startTask(queueName: string, workerName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.isWorking(workerName, queueName))
                .then(isWorking => {
                    if (!isWorking) return reject(`${workerName} is not working on ${queueName}`);

                    const processName = `${workerName}_${queueName}_${uuid.v4()}`;

                    this.redisClient.multi()
                        .rpoplpush(`${this.queues}:${queueName}`, `${this.processes}:${processName}`)
                        .sadd(`${this.workingProcesses}:${workerName}_${queueName}`, processName)
                        .exec(err => {
                            if (err) return reject(err);
                            resolve(processName);
                        });
                });
        });
    }

    public stopTask(processName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const workerName = processName.split('_')[0];
            const queueName = processName.split('_')[1];

            this.redisClient.multi()
                .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                .srem(`${this.workingProcesses}:${workerName}_${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public finishTask(processName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const workerName = processName.split('_')[0];
            const queueName = processName.split('_')[1];

            this.redisClient.multi()
                .del(`${this.processes}:${processName}`)
                .srem(`${this.workingProcesses}:${workerName}_${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public cancelTasks(queueName: string, task: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
                .lrem(`${this.queues}:${queueName}`, 0, task)
                .publish(`${this.queues}:${queueName}_cancel`, task)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    // #endregion

    // #region Workers

    public startWorker(workerName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
                .sadd(this.workers, workerName)
                .set(`${this.workerTimers}:${workerName}`, 'RUN', 'EX', 3)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public keepWorkerAlive(workerName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.set(`${this.workerTimers}:${workerName}`, 'RUN', 'EX', 3, 'XX', (err, success) => {
                if (err) return reject(err);
                if (!success) return this.failWorker(workerName);
                resolve();
            });
        });
    }

    public startWork(workerName: string, queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
                .sadd(`${this.workingQueues}:${workerName}`, queueName)
                .exec(err => {
                    if (err) return reject(err);

                    this.listener.subscribe(`${this.queues}:${queueName}`);

                    resolve();
                });
        });
    }

    public isWorking(workerName: string, queueName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.redisClient.sismember(`${this.workingQueues}:${workerName}`, queueName, (err, isMember) => {
                if (err) return reject(err);
                resolve(isMember === 1);
            });
        });
    }

    public getWorkingQueues(workerName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.redisClient.smembers(`${this.workingQueues}:${workerName}`, (err, queueNames) => {
                if (err) return reject(err);
                resolve(queueNames);
            });
        });
    }

    public getWorkingProcesses(workerName: string, queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.redisClient.smembers(`${this.workingProcesses}:${workerName}_${queueName}`, (err, processNames) => {
                if (err) return reject(err);
                resolve(processNames);
            });
        });
    }

    public stopWork(workerName: string, queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => this.getWorkingProcesses(workerName, queueName))
                .then(processNames => {
                    let multi = this.redisClient.multi()
                        .srem(`${this.workingQueues}:${workerName}`, queueName)
                        .del(`${this.workingProcesses}:${workerName}_${queueName}`);

                    processNames.forEach(processName => {
                        multi = multi
                            .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                            .del(`${this.processes}:${processName}`);
                    });

                    multi.exec(err => {
                        if (err) return reject(err);

                        this.listener.unsubscribe(`${this.queues}:${queueName}`);

                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    public failWorker(workerName: string): Promise<void> {
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
                    let multi = this.redisClient.multi()
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
                        resolve();
                    });
                })
                .catch(err => reject(err));
        });
    }

    // #endregion
}
