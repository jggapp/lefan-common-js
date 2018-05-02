'use strict';
const _ = require('underscore');
const kafka = require('kafka-node');
const Logger = require('../../lib/log/logger');

describe('logger use case test', () => {
    let logger;
    before(() => {
        let options = {
            serviceName: "service-name",
            logType: "console",
            contextKeys:["traceID"]
        }
        logger = new Logger(options);
    });

    describe('#log(message, level, context)', () => {
        it('can log defalut log level message', () => {
            logger.log("this is a message");
        });
        it('can log message', () => {
            logger.log("is a error message", "error", {traceID: "123"});
        });
    });
    describe('#setLevel(level)', () => {
        context('set log level)', () => {
            it('it no log', () => {
                logger.setLevel("error");
                logger.log();
            });
        });
    });
});