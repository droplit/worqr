import * as redis from 'redis';
import { EventEmitter } from 'events';

const log = require('debug')('worqr:');

export class Worqr extends EventEmitter {

    private redisClient: redis.RedisClient;
    private redisKeyPrefix = 'worqr';

    public constructor(redisOptions: {host: string, port: number, options?: redis.ClientOpts}, worqrOptions?: {workerId?: string, workerHeartbeatInterval?: number, workerTimeout?: number, redisKeyPrefix?: string, digestBiteSize?: number}) {
        super();
        this.redisClient = redis.createClient(redisOptions);
    }

    // #region Work

    public enqueue(queueName: string, task: string | string[]) {
        
    }

    // #endregion

    // #region Workers

    public startWork(queueName: string): Promise<boolean> {
        throw new Error('unimplemented');
    }

    public stopWork(queueName: string): Promise<boolean> {
        throw new Error('unimplemented');
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