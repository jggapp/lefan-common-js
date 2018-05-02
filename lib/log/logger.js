'use strict';
const _ = require('underscore');
const log4js = require('log4js');

class Logger {
    constructor(options) {
        let {serviceName = "no-service-name", logType = "console", contextKeys = []} = options;
        this._serviceName = serviceName;
        this._logType = logType;
        let contextPattern = `[%X{serviceName}] `;
        for (let key of contextKeys) {
            contextPattern += `[%X{${key}}] `;
        }
        log4js.configure({
            appenders: {
                "console": {
                    type: 'console', layout: {
                        type: 'pattern',
                        pattern: `%d [%p] ${contextPattern}%m%n`
                    }
                }
            },
            categories: {
                "default": {
                    appenders: ['console'], level: 'info'
                }
            }
        });
        this._logger = log4js.getLogger();
    }

    setLevel(level) {
        switch (level) {
            case "info":
                this._logger.level = 'info';
                return;
            case "error":
                this._logger.level = 'error';
                return;
            case "trace":
                this._logger.level = 'trace';
                return;
            case "debug":
                this._logger.level = 'debug';
                return;
            case "warn":
                this._logger.level = 'warn';
                return;
            case "fatal":
                this._logger.level = 'fatal';
                return;
            default:
                return
        }
    }

    log(message = "no message", level = "info", context = {}) {
        switch (level) {
            case "info":
                this.info(message, context);
                return;
            case "error":
                this.error(message, context);
                return;
            case "trace":
                this.trace(message, context);
                return;
            case "debug":
                this.debug(message, context);
                return;
            case "warn":
                this.warn(message, context);
                return;
            case "fatal":
                this.fatal(message, context);
                return;
            default:
                return
        }
    }

    _addContext(context) {
        this._logger.addContext('serviceName', this._serviceName);
        if (context) {
            for (let key of _.keys(context)) {
                this._logger.addContext(key, context[key]);
            }
        }
    }

    info(message = "no message", context = {}) {
        this._addContext(context);
        this._logger.info(message);
    }

    error(message = "no message", context = {}) {
        this._addContext(context);
        this._logger.error(message);
    }

    trace(message = "no message", context = {}) {
        this._addContext(context);
        this._logger.trace(message);
    }

    debug(message = "no message", context = {}) {
        this._addContext(context);
        this._logger.debug(message);
    }

    warn(message = "no message", context = {}) {
        this._addContext(context);
        this._logger.warn(message);
    }

    fatal(message = "no message", context = {}) {
        this._addContext(context);
        this._logger.fatal(message);
    }
}

module.exports = Logger;