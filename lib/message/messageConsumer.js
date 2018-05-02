'use strict';
const _ = require('underscore');
const kafka = require('kafka-node');

class MessageConsumer {
    constructor({serviceName = "no-service-name", consumerOpts = {}}) {
        this.serviceName = serviceName;
        let {
            kafkaHost = process.env.KAFKA_HOST ? process.env.KAFKA_HOST : `127.0.0.1:9092`,
            groupId = `${serviceName}-consume-group`,
            autoCommit = false,
            sessionTimeout = 30000,
            protocol = ['roundrobin'],
            fromOffset = 'latest',
            outOfRangeOffset = 'latest'
        } = consumerOpts;
        this.consumerOpts = {
            kafkaHost,
            groupId,
            autoCommit,
            sessionTimeout,
            protocol,
            fromOffset,
            outOfRangeOffset
        };
    }

    consumeMessage(topics, callback) {
        if (!topics) {
            callback(new Error("topics is lack or fail"));
            return;
        }
        this.consumer = new kafka.ConsumerGroup(this.consumerOpts, topics);
        this.consumer.on('error', err => {
            callback(err);
        });
        this.consumer.on('offsetOutOfRange', err => {
            callback(err);
        });
        this.consumer.on('message', message => {
            callback(null, message);
        });
    }

    commit(callback) {
        if (!this.consumer) {
            callback(new Error(`${this.serviceName} is no consumer for kafka`));
            return;
        }
        let self = this;

        function manuallyCommit() {
            return new Promise((resolve, reject) => {
                self.consumer.commit((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data);
                    }
                });
            });
        }

        async function execute() {
            let result = await manuallyCommit();
            return result;
        };
        if (callback && _.isFunction(callback)) {
            execute().then(result => {
                callback(null, result);
            }).catch(err => {
                callback(err);
            });
        }
    }

    close(force = false, callback) {
        if (!this.consumer) {
            callback(new Error(`${this.serviceName} is no consumer for kafka`));
            return;
        }
        let self = this;

        function clientClose() {
            return new Promise((resolve, reject) => {
                self.consumer.close(force, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }

        async function execute() {
            await clientClose();
        };
        if (callback && _.isFunction(callback)) {
            execute().then(() => {
                callback(null);
            }).catch(err => {
                callback(err);
            });
        }
    }
}

module.exports = MessageConsumer;