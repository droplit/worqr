import { RedisClient } from 'redis';

/**
 * Options for Redis connection.
 * You can use an existing Redis connection by supplying it in the `data` and `subscribe` properties.
 * Otherwise, you can create a new connection by filling in `host`, `port`, and `password`.
 */
export interface RedisOptions {
    data?: RedisClient;
    subscribe?: RedisClient;
    host: string;
    port: number;
    password?: string;
}

/**
 * Options for the Worqr instance.
 */
export interface WorqrOptions {
    redisKeyPrefix?: string;
    instanceId?: string;
    workerHeartbeatInterval?: number;
    workerTimeout?: number;
    workerCleanupInterval?: number;
    digestBiteSize?: number;
}

/**
 * Represents a process started by a worker.
 */
export interface Process {
    id: string;
    task: string;
}
