import { RedisClient } from 'redis';

/**
 * Options for Redis connection.
 * You can use an existing Redis connection by supplying it in the `data` and `subscribe` properties.
 * Otherwise, you can create a new connection by filling in `host`, `port`, and `password`.
 */
export interface RedisOptions {
    /** An existing publish connection. */
    data?: RedisClient;
    /** An existing subscribe connection. */
    subscribe?: RedisClient;
    /** The host name. */
    host?: string;
    /** The port. */
    port?: number;
    /** The server password. */
    password?: string;
}

/**
 * Options for the Worqr instance.
 */
export interface WorqrOptions {
    /** The prefix to use in redis. Defaults to `worqr`. */
    redisKeyPrefix?: string;
    /** The unique ID of this instance. Defaults to a random UUID. */
    workerId?: string;
    /** How often (in milliseconds) to refresh this worker's timer. Defaults to `1000`. */
    workerHeartbeatInterval?: number;
    /** How long (in seconds) the timer should be set to. Defaults to `3`. */
    workerTimeout?: number;
    /** How often (in milliseconds) to check for dead workers. Defaults to `10000`. */
    workerCleanupInterval?: number;
}

/**
 * Represents a process started by a worker.
 */
export interface Process {
    /** The unique ID. */
    id: string;
    /** The task payload. */
    task: string;
}
