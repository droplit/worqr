import { Worqr } from '../index';
//import Worqr from '../worqr';

require('dotenv').config();

const worqr = new Worqr({host: <string>process.env.REDIS_HOST, port: Number.parseInt(<string>process.env.REDIS_PORT), options: {password: process.env.REDIS_PASSWORD}}, {redisKeyPrefix: 'worq1'});

const MY_WORK = 'work';

worqr.createQueue(MY_WORK).then((created) => {
    worqr.startWork(MY_WORK).then((working) => {

    });
});