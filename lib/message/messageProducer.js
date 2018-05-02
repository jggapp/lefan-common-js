'use strict';
const _ = require('underscore');
const kafka = require('kafka-node');

class MessageProducer {
    constructor({clientOpts = {}, producerOpts = {}}) {
        let {
            kafkaHost = process.env.KAFKA_HOST ? process.env.KAFKA_HOST : `127.0.0.1:9092`,
            connectTimeout = 10000,
            requestTimeout = 30000,
            autoConnect = true,
        } = clientOpts;
        let {
            requireAcks = -1,
            ackTimeoutMs = 3000,
            partitionerType = 3
        } = producerOpts;
        this.client = new kafka.KafkaClient({
            kafkaHost,
            connectTimeout,
            requestTimeout,
            autoConnect
        });
        let self = this;
        this.producerPromise = new Promise((resolve, reject) => {
            let producer = new kafka.HighLevelProducer(self.client, {
                requireAcks,
                ackTimeoutMs,
                partitionerType
            });
            producer.on('ready', () => {
                resolve(producer);
            });
            producer.on('error', err => {
                if (self.client) {
                    self.client.close();
                }
                reject(err);
            });
        });
    }

    createTopics(topics, callback) {
        if (!topics) {
            if (callback && _.isFunction(callback)) {
                callback(new Error("topics is lack or fail"));
                return;
            }
            else {
                return new Promise((resolve, reject) => {
                    reject(new Error("topics is lack or fail"));
                });
            }
        }
        let self = this;

        function createTopic(producer) {
            return new Promise((resolve, reject) => {
                producer.createTopics(topics, false, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data);
                    }
                });
            });
        }

        function refreshTopic() {
            return new Promise((resolve, reject) => {
                self.client.refreshMetadata(topics, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }

        async function execute() {
            let producer = await self.producerPromise;
            let data = await createTopic(producer);
            await refreshTopic();
            return data
        };
        if (callback && _.isFunction(callback)) {
            execute().then((result) => {
                callback(null, result);
            }).catch(err => {
                callback(err);
            });
        }
        else {
            return execute();
        }
    }

    produceMessage(topic, message, callback) {
        if (!topic || !message) {
            if (callback && _.isFunction(callback)) {
                callback(new Error("topic or message is lack or fail"));
                return;
            }
            else {
                return new Promise((resolve, reject) => {
                    reject(new Error("topic or message is lack or fail"));
                });
            }
        }
        let self = this;
        let kafkaMessage;
        if (_.isObject(message)) {
            if (message.key && message.value) {
                let value;
                if (_.isObject(message.value)) {
                    value = JSON.stringify(message.value);
                } else {
                    value = message.value;
                }
                kafkaMessage = new kafka.KeyedMessage(message.key, value);
            }
            else {
                kafkaMessage = JSON.stringify(message);
            }
        }
        else {
            kafkaMessage = message;
        }
        var payloads = [{
            topic: topic,
            messages: kafkaMessage,
            attributes: 0,
            timestamp: Date.now()
        }];

        function sendMessage(producer) {
            return new Promise((resolve, reject) => {
                producer.send(payloads, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data);
                    }
                });
            });
        }

        async function execute() {
            let producer = await self.producerPromise;
            let data = await sendMessage(producer);
            return data
        };
        if (callback && _.isFunction(callback)) {
            execute().then((result) => {
                callback(null, result);
            }).catch(err => {
                callback(err);
            });
        }
        else {
            return execute();
        }
    }

    close(callback) {
        let self = this;

        function clientClose() {
            return new Promise((resolve, reject) => {
                self.client.close((err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }

        async function execute() {
            await self.producerPromise;
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

module.exports = MessageProducer;