import { Worqr } from '../index';
//import Worqr from '../worqr';

require('dotenv').config();

const worqr = new Worqr({ host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), options: { password: process.env.REDIS_PASSWORD } }, { redisKeyPrefix: 'worq1' });

worqr.enqueue('myQueue', 'myTask').then(() => {
    worqr.startTask('myQueue', 'myWorker').then(processName => {
        console.log(processName);
    });
});

worqr.startWork('myWorker', 'myQueue').then(() => {

});