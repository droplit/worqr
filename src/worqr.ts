import * as crypto from 'crypto';
import debug from 'debug';
import { EventEmitter } from 'events';
import * as redis from 'redis';
import * as uuid from 'uuid';
import { Process, Options } from './types';

const log = debug('worqr');

const WORKER_RUN_KEY = 'RUN';

/**
 * A distributed, reliable, job queueing system that uses redis as a backend.
 */
export class Worqr extends EventEmitter {
    /** Options given to constructor */
    private options: Options;
    /** The publish connection. */
    private pub: ReturnType<typeof redis.createClient>;
    /** The subscribe connection. */
    private sub: ReturnType<typeof redis.createClient>;
    /** The prefix to use in redis. */
    private redisKeyPrefix = 'worqr';
    /** The unique ID of this instance. */
    private workerId = uuid.v4();
    /** How often (in milliseconds) to refresh this worker's timer. */
    private workerHeartbeatInterval = 1000;
    /** How long (in seconds) the timer should be set to. */
    private workerTimeout = 3;
    /** How often (in milliseconds) to check for dead workers. */
    private workerCleanupInterval = 10000;
    /** The NodeJS timer for the heartbeat. */
    private workerTimerIntervalId?: NodeJS.Timer;

    /** Queue prefix. */
    private queues: string;
    /** Unique task prefix. */
    private uniqueTasks: string;
    /** Process prefix. */
    private processes: string;
    /** Worker prefix. */
    private workers: string;
    /** Expiring worker prefix. */
    private expiringWorkers: string;
    /** Permanent worker prefix. */
    private permanentWorkers: string;
    /** Worker timer prefix. */
    private workerTimers: string;
    /** Working queue prefix. */
    private workingQueues: string;
    /** Working process prefix. */
    private workingProcesses: string;

    /** Run the worker in debug mode to gather performance metrics. */
    private debugMode = false;
    /** How often (in milliseconds) to emit debug events in debug mode. */
    private debugInterval = 1000;
    /** Debug information for this worker. */
    private debugInfo: { jobs?: Record<string, { time?: number, start: number }> } = {};

    /**
     * Creates a Worqr instance.
     * @param options
     */
    constructor(options: Options) {
        super();

        this.options = options;

        this.pub = options.redis?.data || redis.createClient(options.redis?.redisClientOptions);
        this.pub.on('error', error => this.emit('error', error));

        this.sub = options.redis?.subscribe || redis.createClient(options.redis?.redisClientOptions);
        this.sub.on('error', error => this.emit('error', error));

        this.redisKeyPrefix = options.worqr?.redisKeyPrefix || this.redisKeyPrefix;
        this.workerId = options.worqr?.workerId || this.workerId;
        this.workerHeartbeatInterval = options.worqr?.workerHeartbeatInterval || this.workerHeartbeatInterval;
        this.workerTimeout = options.worqr?.workerTimeout || this.workerTimeout;
        this.workerCleanupInterval = options.worqr?.workerCleanupInterval || this.workerCleanupInterval;

        this.queues = `${this.redisKeyPrefix}:queues`;
        this.uniqueTasks = `${this.redisKeyPrefix}:uniqueTasks`;
        this.processes = `${this.redisKeyPrefix}:processes`;
        this.workers = `${this.redisKeyPrefix}:workers`;
        this.expiringWorkers = `${this.redisKeyPrefix}:expiringWorkers`;
        this.permanentWorkers = `${this.redisKeyPrefix}:permanentWorkers`;
        this.workerTimers = `${this.redisKeyPrefix}:workerTimers`;
        this.workingQueues = `${this.redisKeyPrefix}:workingQueues`;
        this.workingProcesses = `${this.redisKeyPrefix}:workingProcesses`;

        this.debugMode = options.worqr?.debugMode || this.debugMode;
        this.debugInterval = options.worqr?.debugInterval || this.debugInterval;
    }

    public async init() {
        if (!this.options.redis?.data)
            await this.pub.connect();
        if (!this.options.redis?.subscribe)
            await this.sub.connect();

        if (this.debugMode) {
            setInterval(() => {
                this.debugInfo.jobs = this.debugInfo.jobs || {};
                const jobs = { ...this.debugInfo.jobs };
                Object.keys(jobs).forEach(processId => {
                    const job = jobs[processId];
                    jobs[processId] = {
                        start: job.start,
                        time: Date.now() - job.start,
                    };
                });
                this.emit('debug', {
                    jobCount: Object.keys(jobs).length,
                    jobs,
                });
            }, this.debugInterval);
        }

        setInterval(() => {
            this.cleanupWorkers();
        }, this.workerCleanupInterval);
    }

    private onMessage(message: string, channel: string) {
        const unprefixedChannel = channel.substring(channel.indexOf('_') + 1);
        const lastUnderscore = unprefixedChannel.lastIndexOf('_');
        const queueName = unprefixedChannel.substring(0, lastUnderscore);
        const type = unprefixedChannel.substring(lastUnderscore + 1);
        this.emit(queueName, type, message);
    }

    // #region Queues

    /**
     * Returns a list of all queues.
     * @returns The queue names.
     */
    public async getQueues() {
        return (await this.pub.keys(`${this.queues}*`)).map(queueName => queueName.split(':')[2]);
    }

    /**
     * Returns a list of queues a worker is working on.
     * @param workerId The worker ID (ID of the instance if unspecified).
     * @returns The queue names.
     */
    private async getWorkingQueues(workerId: string = this.workerId) {
        return this.pub.sMembers(`${this.workingQueues}:${workerId}`);
    }

    /**
     * Returns the next task in a queue.
     * @param queueName The name of the queue.
     * @returns The next task.
     */
    public async peekQueue(queueName: string) {
        return this.pub.lIndex(`${this.queues}:${queueName}`, -1);
    }

    /**
     * Returns the number of tasks in a queue.
     * @param queueName The name of the queue.
     * @returns The number of tasks.
     */
    public async countQueue(queueName: string) {
        return this.pub.lLen(`${this.queues}:${queueName}`);
    }

    /**
     * Deletes a queue, emitting an event with `type: 'delete'`.
     * Clients should listen for this event and stop work on the queue.
     * @param queueName The name of the queue.
     */
    public async deleteQueue(queueName: string) {
        log(`deleting ${queueName}`);
        return this.pub.multi()
            .del(`${this.queues}:${queueName}`)
            .publish(`${this.redisKeyPrefix}_${queueName}_delete`, '')
            .exec();
    }

    // #endregion

    // #region Tasks

    /**
     * Enqueues a task on a queue, emitting an event with `type: 'work'`.
     * Clients should listen for this event and start tasks on the queue.
     * @param queueName The name of the queue.
     * @param task A single task or array of tasks.
     * @param unique Whether the task is unique.
     * If set to true, the promise will reject when trying to enqueue a duplicate unique task.
     * A queue can consist of a mix of unique and non-unique tasks.
     * Enqueueing a unique task that matches a non-unique task will NOT reject.
     */
    public async enqueue(queueName: string, task: string | string[], unique = false) {
        log(`queueing ${task.toString()} to ${queueName}`);

        if (!unique) {
            await this.pub.multi()
                .lPush(`${this.queues}:${queueName}`, task)
                .publish(`${this.redisKeyPrefix}_${queueName}_work`, Array.isArray(task) ? task.length.toString() : '1')
                .exec();
            return;
        }
        if (Array.isArray(task)) {
            throw new Error('Unique task array not currently supported');
        }
        const taskHash = crypto.createHash('md5').update(task || '').digest('hex').toString();

        await this.pub.watch(`${this.uniqueTasks}:${taskHash}`);
        const existingTask = await this.pub.get(`${this.uniqueTasks}:${taskHash}`);
        if (existingTask) throw new Error('Trying to enqueue duplicate task in unique queue');

        await this.pub.multi()
            .set(`${this.uniqueTasks}:${taskHash}`, ' ')
            .lPush(`${this.queues}:${queueName}`, task)
            .publish(`${this.redisKeyPrefix}_${queueName}_work`, '1')
            .exec();
    }

    /**
     * Returns all the tasks in a queue.
     * @param queueName The name of the queue.
     * @returns The tasks.
     */
    public async getTasks(queueName: string) {
        return this.pub.lRange(`${this.queues}:${queueName}`, 0, -1);
    }

    /**
     * Returns the task for a given process.
     * @param queueName The process ID.
     * @returns The task.
     */
    private async getTaskForProcess(processId: string) {
        return this.pub.lIndex(`${this.processes}:${processId}`, 0);
    }

    /**
     * Removes a task from the queue, emitting an event with `type: 'cancel'` and  `message: <task>`.
     * Clients should listen for this event and stop all processes matching the task.
     * @param queueName The name of the queue.
     * @param task The task to cancel.
     */
    public async cancelTasks(queueName: string, task: string) {
        log(`canceling ${task} on ${queueName}`);
        await this.pub.multi()
            .lRem(`${this.queues}:${queueName}`, 0, task)
            .publish(`${this.redisKeyPrefix}_${queueName}_cancel`, task)
            .exec();
    }

    // #endregion

    // #region Processes

    /**
     * Dequeues a task from the queue, returning a process.
     * The worker must have started work on the queue in order to get tasks.
     * Process will be null if the queue is empty.
     * @param queueName The name of the queue.
     * @returns A process (null if there is no task).
     */
    public async dequeue(queueName: string): Promise<Process | null> {
        log(`${this.workerId} starting task on ${queueName}`);

        const isWorking = await this.isWorking(this.workerId, queueName)
        if (!isWorking) {
            throw new Error(`${this.workerId} is not working on ${queueName}`);
        }

        const processId = `${queueName}_${uuid.v4()}`;

        const results = await this.pub.multi()
            .rPopLPush(`${this.queues}:${queueName}`, `${this.processes}:${processId}`)
            .sAdd(`${this.workingProcesses}:${queueName}`, processId)
            .exec();

        if (!results) {
            throw new Error('Failed to exec multi');
        }
        const [task] = results;
        if (this.debugMode) {
            this.debugInfo.jobs = this.debugInfo.jobs || {};
            this.debugInfo.jobs[processId] = {
                start: Date.now(),
            };
        }
        if (task) {
            const process: Process = ({ id: processId, task: task.toString() });
            return process;
        }

        // Queue empty
        await this.pub.multi()
            .rPopLPush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
            .del(`${this.processes}:${processId}`)
            .sRem(`${this.workingProcesses}:${queueName}`, processId)
            .exec();

        if (this.debugMode) {
            this.debugInfo.jobs = this.debugInfo.jobs || {};
            delete this.debugInfo.jobs[processId];
        }
        return null;
    }

    /**
     * Returns a list of all processes running.
     * @returns The process IDs.
     */
    private async getProcesses() {
        return (await this.pub.keys(`${this.processes}*`)).map(processId => processId.split(':')[2])
    }

    /**
     * Returns a list of processes for tasks matching the given task.
     * @param task The task to match.
     * @returns The process IDs.
     */
    public async getMatchingProcesses(task: string) {
        const processIds = await this.getProcesses();
        const processIdsForTask = (await Promise.all(
            processIds.map(this.getTaskForProcess)
        )).filter((t): t is string => t === task);
        return processIdsForTask;
    }

    /**
     * Returns a list of all processes running on a queue.
     * @param queueName The name of the queue.
     * @returns The process IDs.
     */
    private async getWorkingProcesses(queueName: string) {
        return this.pub.sMembers(`${this.workingProcesses}:${queueName}`);
    }

    /**
     * Stops a process, returning its task to the queue it came from.
     * @param processId The process ID.
     */
    public async stopProcess(processId: string) {
        log(`stopping process ${processId}`);
        const queueName = processId.substring(0, processId.lastIndexOf('_'));
        await this.pub.multi()
            .rPopLPush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
            .sRem(`${this.workingProcesses}:${queueName}`, processId)
            .exec();
    }

    /**
     * Stops a process, removing the task entirely.
     * @param processId The process ID.
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    public async finishProcess(processId: string, result?: any) {
        log(`finishing process ${processId}`);
        const queueName = processId.substring(0, processId.lastIndexOf('_'));
        const [task] = await this.pub.lRange(`${this.processes}:${processId}`, 0, -1);
        const taskHash = crypto.createHash('md5').update(task || '').digest('hex').toString();

        await this.pub.multi()
            .del(`${this.uniqueTasks}:${taskHash}`)
            .del(`${this.processes}:${processId}`)
            .sRem(`${this.workingProcesses}:${queueName}`, processId)
            .exec();

        const message = { workerId: this.workerId, task, result };
        await this.pub.publish(`${this.redisKeyPrefix}_${queueName}_done`, JSON.stringify(message));
        if (this.debugMode) {
            this.debugInfo.jobs = this.debugInfo.jobs || {};
            delete this.debugInfo.jobs[processId];
        }
    }

    // #endregion

    // #region Worker

    /**
     * Returns the unique worker ID for this instance.
     * @returns The worker ID.
     */
    public getWorkerId(): string {
        return this.workerId;
    }

    /**
     * Starts this worker.
     */
    public async startWorker(): Promise<void> {
        log(`starting worker ${this.workerId}`);

        let multi = this.pub.multi()
            .sAdd(this.workers, this.workerId);
        if (this.workerTimeout < 0) {
            multi = multi.sAdd(this.permanentWorkers, this.workerId);
        } else {
            multi = multi
                .sAdd(this.expiringWorkers, this.workerId)
                .set(`${this.workerTimers}:${this.workerId}`, WORKER_RUN_KEY, {
                    'EX': this.workerTimeout
                });
        }

        await multi.exec();

        this.workerTimerIntervalId = setInterval(() => {
            this.keepWorkerAlive();
        }, this.workerHeartbeatInterval);
    }

    /**
     * If a subscriber is not specified, create one.
     * Starts work on a queue.
     * The worker must be started before starting work on a queue.
     * The worker will start emitting events for the queue, which clients should subscribe to.
     * This also emits an event immediately if there are tasks on the queue.
     * @param queueName The name of the queue.
     */
    public async startWork(queueName: string): Promise<void> {
        log(`${this.workerId} starting work on ${queueName}`);

        const isStarted = await this.isStarted(this.workerId);
        if (!isStarted) {
            throw new Error(`${this.workerId} is not started`);
        }

        await this.pub.sAdd(`${this.workingQueues}:${this.workerId}`, queueName)

        await this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_work`, this.onMessage.bind(this));
        await this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_done`, this.onMessage.bind(this));
        await this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_cancel`, this.onMessage.bind(this));
        await this.sub.subscribe(`${this.redisKeyPrefix}_${queueName}_delete`, this.onMessage.bind(this));
        await this.requestWork(queueName);
    }

    /**
     * Requests a task from the queue.
     * If there is one, an event with `type: 'work'` will be published on that queue's channel.
     * `message` will be the count of tasks on the queue.
     * This is so the client doesn't have to set up their own polling of the queue.
     * @param queueName The name of the queue.
     */
    public async requestWork(queueName: string) {
        const count = await this.countQueue(queueName);
        if (count > 0) {
            await this.pub.publish(`${this.redisKeyPrefix}_${queueName}_work`, count.toString());
        }
    }

    /**
     * Stops work on a queue.
     * The worker will stop emitting events for the queue.
     * @param queueName The name of the queue.
     */
    public async stopWork(queueName: string): Promise<void> {
        log(`${this.workerId} stopping work on ${queueName}`);
        const processIds = await this.getWorkingProcesses(queueName);

        const multi = this.pub.multi()
            .sRem(`${this.workingQueues}:${this.workerId}`, queueName)
            .del(`${this.workingProcesses}:${queueName}`);

        processIds.forEach(processId => {
            multi
                .rPopLPush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
                .del(`${this.processes}:${processId}`);
        });

        await multi.exec();

        await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
        await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_done`);
        await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);
        await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_delete`);
    }

    /**
     * Refreshes this worker's timer, indicating it is still active.
     */
    private async keepWorkerAlive() {
        if (this.workerTimeout <= 0) {
            return;
        }
        const success = await this.pub.set(`${this.workerTimers}:${this.workerId}`, WORKER_RUN_KEY, { 'EX': this.workerTimeout, 'XX': true })
        if (!success) {
            this.failWorker(this.workerId)
            return;
        }
        return;
    }

    // #endregion

    // #region Workers

    /**
     * Returns a list of all workers.
     * @returns The worker IDs.
     */
    public async getWorkers() {
        return this.pub.sMembers(this.workers);
    }

    /**
     * Returns whether a worker is started.
     * @param workerId The worker ID.
     * @returns Whether the worker is started.
     */
    private async isStarted(workerId: string) {
        return this.pub.sIsMember(this.workers, workerId);
    }

    /**
     * Returns whether a worker is working on a queue.
     * @param workerId The worker ID.
     * @param queueName The name of the queue.
     * @returns Whether the worker is working on the queue.
     */
    private async isWorking(workerId: string, queueName: string): Promise<boolean> {
        return this.pub.sIsMember(`${this.workingQueues}:${workerId}`, queueName);
    }

    /**
     * Fails a worker, putting all its tasks back on the queues they came from.
     * @param workerId The worker ID (ID of the instance if unspecified).
     */
    public async failWorker(workerId: string = this.workerId): Promise<void> {
        log(`failing ${workerId}`);

        const queueNames = await this.getWorkingQueues(workerId);

        const processIds: string[] = [];
        await Promise.all(queueNames.map(queueName => this.getWorkingProcesses(queueName)))
            .then(processIds2d => processIds2d.forEach(processIds1d => processIds.push(...processIds1d)));

        const multi = this.pub.multi()
            .sRem(this.workers, workerId)
            .sRem(this.expiringWorkers, workerId)
            .sRem(this.permanentWorkers, workerId)
            .del(`${this.workerTimers}:${workerId}`)
            .del(`${this.workingQueues}:${workerId}`);

        queueNames.forEach(queueName => {
            multi.del(`${this.workingProcesses}:${queueName}`);
        });

        processIds.forEach(processId => {
            const queueName = processId.substr(0, processId.lastIndexOf('_'));
            multi
                .rPopLPush(`${this.processes}:${processId}`, `${this.queues}:${queueName}`)
                .del(`${this.processes}:${processId}`);
        });

        await multi.exec();

        if (workerId === this.workerId) {
            await Promise.all(queueNames.map(async queueName => {
                await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_work`);
                await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_done`);
                await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_cancel`);
                await this.sub.unsubscribe(`${this.redisKeyPrefix}_${queueName}_delete`);
            }));
        }
    }

    /**
     * Returns a list of all expiring workers.
     * @returns The worker IDs.
     */
    private async getExpiringWorkers() {
        return this.pub.sMembers(this.expiringWorkers);
    }

    /**
     * Fails all workers that have timed out.
     * This is run every `workerCleanupInterval` milliseconds.
     */
    public async cleanupWorkers(): Promise<void> {
        const workerIds = await this.getExpiringWorkers();

        const results = await Promise.all(
            workerIds.map(async workerId => {
                const workerTimers = await this.pub.keys(`${this.workerTimers}:${workerId}`);
                return { workerId, dead: workerTimers.length === 0 };
            })
        );

        await Promise.all(
            results
                .filter(({ workerId, dead }) => workerId !== this.workerId && dead)
                .map(({ workerId }) => this.failWorker(workerId))
        );
    }

    // #endregion
}
