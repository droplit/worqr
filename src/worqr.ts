import * as redis from 'redis';
import { EventEmitter } from 'events';
import * as uuid from 'uuid';

const log = require('debug')('worqr');

export class Worqr extends EventEmitter {

    private redisClient: redis.RedisClient;
    private redisKeyPrefix = 'worqr';
    private queues: string;
    private processes: string;
    private workers: string;
    private workerQueues: string;
    private workerTimers: string;
    private workmap: string;

    public constructor(redisOptions: { host: string, port: number, options?: redis.ClientOpts }, worqrOptions?: { workerId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, redisKeyPrefix?: string, digestBiteSize?: number }) {
        super();
        this.redisClient = redis.createClient(redisOptions);
        this.redisKeyPrefix = (worqrOptions && worqrOptions.redisKeyPrefix) || this.redisKeyPrefix;
        this.queues = `${this.redisKeyPrefix}:queues`;
        this.processes = `${this.redisKeyPrefix}:processes`;
        this.workers = `${this.redisKeyPrefix}:workers`;
        this.workerQueues = `${this.redisKeyPrefix}:workerQueues`;
        this.workerTimers = `${this.redisKeyPrefix}:workerTimers`;
        this.workmap = `${this.redisKeyPrefix}:workmap`;

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

    // why use this function? enqueue does the same thing
    // also cannot create an empty list in redis, should i create a list with a dummy task?
    public createQueue(queueName: string): Promise<void> {
        throw new Error('unimplemented');
    }

    public enqueue(queueName: string, task: string | string[]): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.lpush(`${this.queues}:${queueName}`, task, err => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    // is this supposed to return a list of tasks in the queue, or a list of all queues?
    // if the former, it should be renamed to getQueueTasks
    // if the latter, why does it take queueName as a parameter?
    public getQueueNames(queueName: string): Promise<string[]> {
        return new Promise((resolve, reject) => {
            this.redisClient.lrange(`${this.queues}:${queueName}`, 0, -1, (err, tasks) => {
                if (err) return reject(err);
                resolve(tasks);
            });
        });
    }

    // not sure how to check that
    public isQueue(queueName: string): Promise<void> {
        throw new Error('unimplemented');
    }

    public deleteQueue(queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
                .del(`${this.queues}:${queueName}`)
                // need to do a lot of other stuff
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
            const processName = `${workerName}_${queueName}_${uuid.v4()}`;

            this.redisClient.multi()
                .rpoplpush(`${this.queues}:${queueName}`, `${this.processes}:${processName}`)
                .sadd(`${this.workerQueues}:${workerName}_${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve(processName);
                });
        });
    }

    public stopTask(processName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const workerName = processName.split('_')[0];
            const queueName = processName.split('_')[1];

            this.redisClient.multi()
                .rpoplpush(`${this.processes}:${processName}`, `${this.queues}:${queueName}`)
                .srem(`${this.workerQueues}:${workerName}_${queueName}`, processName)
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
                .del(processName)
                .srem(`${this.workerQueues}:${workerName}_${queueName}`, processName)
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
            this.redisClient.sadd(`${this.workmap}:${workerName}`, queueName, err => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    public stopWork(workerName: string, queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => new Promise<string[]>((resolve, reject) => {
                    this.redisClient.smembers(`${this.workerQueues}:${workerName}_${queueName}`, (err, processNames) => {
                        if (err) return reject(err);
                        resolve(processNames);
                    });
                }))
                .then(processNames => {
                    let multi = this.redisClient.multi()
                        .srem(`${this.workmap}:${workerName}`, queueName)
                        .del(`${this.workerQueues}:${workerName}_${queueName}`);

                    processNames.forEach(processName => {
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

    public failWorker(workerName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.resolve()
                .then(() => new Promise<string[]>((resolve, reject) => {
                    this.redisClient.smembers(`${this.workmap}:${workerName}`, (err, queueNames) => {
                        if (err) return reject(err);
                        resolve(queueNames);
                    });
                }))
                .then(queueNames => new Promise<[string[], string[]]>((resolve, reject) => {
                    const allProcessNames: string[] = [];

                    Promise.all(queueNames.map(queueName => new Promise((resolve, reject) => {
                        this.redisClient.smembers(`${this.workerQueues}:${workerName}_${queueName}`, (err, processNames) => {
                            if (err) return reject(err);

                            allProcessNames.push(...processNames);

                            resolve();
                        });
                    })))
                        .then(() => resolve([queueNames, allProcessNames]))
                        .catch(err => reject(err));
                }))
                .then(([queueNames, processNames]) => {
                    let multi = this.redisClient.multi()
                        .srem(this.workers, workerName)
                        .del(`${this.workerTimers}:${workerName}`)
                        .del(`${this.workmap}:${workerName}`);

                    queueNames.forEach(queueName => {
                        multi = multi
                            .del(`${this.workerQueues}:${workerName}_${queueName}`);
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