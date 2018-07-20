import * as redis from 'redis';
import { EventEmitter } from 'events';
import * as uuid from 'uuid';

const log = require('debug')('worqr:');

export class Worqr extends EventEmitter {

    private redisClient: redis.RedisClient;
    private redisKeyPrefix = 'worqr';

    public constructor(redisOptions: { host: string, port: number, options?: redis.ClientOpts }, worqrOptions?: { workerId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, redisKeyPrefix?: string, digestBiteSize?: number }) {
        super();
        this.redisClient = redis.createClient(redisOptions);
        if (worqrOptions && worqrOptions.redisKeyPrefix) {
            this.redisKeyPrefix = worqrOptions.redisKeyPrefix;
        }

        // digest
        setInterval(() => {
        }, 1000);
    }

    // #region Tasks

    public enqueue(queueName: string, task: string | string[]): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.lpush(`${this.redisKeyPrefix}:queues:${queueName}`, task, err => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    public startTask(queueName: string, workerName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            const processName = `${this.redisKeyPrefix}:processes:${workerName}_${queueName}_${uuid.v4()}`;

            this.redisClient.multi()
                .rpoplpush(`${this.redisKeyPrefix}:queues:${queueName}`, processName)
                .sadd(`${this.redisKeyPrefix}:worker_queues:${workerName}_${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve(processName);
                });
        });
    }

    public stopTask(processName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const workerName = processName.split(':')[2].split('_')[0];
            const queueName = processName.split(':')[2].split('_')[1];

            this.redisClient.multi()
                .rpoplpush(processName, `${this.redisKeyPrefix}:queues:${queueName}`)
                .srem(`${this.redisKeyPrefix}:worker_queues:${workerName}_${queueName}`, processName)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public finishTask(processName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const workerName = processName.split(':')[2].split('_')[0];
            const queueName = processName.split(':')[2].split('_')[1];

            this.redisClient.multi()
                .del(processName)
                .srem(`${this.redisKeyPrefix}:worker_queues:${workerName}_${queueName}`, processName)
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
                .sadd(`${this.redisKeyPrefix}:workers`, workerName)
                .set(workerName, 'RUN', 'EX', 3)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public keepWorkerAlive(workerName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.set(workerName, 'RUN', 'EX', 3, 'XX', err => {
                if (err) return resolve(); // restart worker
                resolve();
            });
        });
    }

    public startWork(workerName: string, queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.sadd(`${this.redisKeyPrefix}:workmap:${workerName}`, queueName, err => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    public stopWork(workerName: string, queueName: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.redisClient.multi()
                .srem(`${this.redisKeyPrefix}:workmap:${workerName}`, queueName)
                // step 4 of failWorker
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }

    public failWorker(workerName: string): Promise<void> {
        return new Promise((resolve, reject) => {

        });
    }

    // #endregion

    // #region Queues

    public createQueue(queueName: string): Promise<boolean> {
        throw new Error('unimplemented');
    }

    public getQueueNames(queueName: string): Promise<string[]> {
        throw new Error('unimplemented');
    }

    public isQueue(queueName: string): Promise<boolean> {
        throw new Error('unimplemented');
    }

    public deleteQueue(queueName: string): Promise<boolean> {
        throw new Error('unimplemented');
    }

    // #endregion

}