'use strict';
const _ = require('underscore');
const kafka = require('kafka-node');
const expect = require('chai').expect;
const MessageProducer = require('../../lib/message/messageProducer');

describe('MessageProducer(options) use case test', () => {
    let messageProducer;
    let kafkaConsumer;
    before(done => {
        let options = {};
        messageProducer = new MessageProducer(options);
        kafkaConsumer = new kafka.ConsumerGroup({
            kafkaHost: "127.0.0.1:9092",
            groupId: "test-group",
            sessionTimeout: 15000,
            protocol: ['roundrobin'],
            fromOffset: 'latest',
            outOfRangeOffset: 'earliest'
        }, ["test-topic", "test-key-topic"]);
        done();
    });
    after(done => {
        kafkaConsumer.close(true, () => {
            messageProducer.close(done);
        });
    });
    describe('#createTopics(topics,callback)', () => {
        it('is return a err if topics is lack or fail', done => {
            let topics = null;
            messageProducer.createTopics(topics, (err, result) => {
                expect(err.message).to.equal("topics is lack or fail");
                expect(result).to.be.undefined;
                done();
            })
        });
        it('is ok', done => {
            let topics = ["test-topic", "test-key-topic"];
            messageProducer.createTopics(topics, (err, result) => {
                expect(err).to.be.null;
                done();
            })
        });
    });
    describe('#produceMessage(topic, message, callback)', () => {
        it('is return a err if topic or message is lack or fail', done => {
            let topic = null;
            let message = null;
            messageProducer.produceMessage(topic, message, (err, result) => {
                expect(err.message).to.equal("topic or message is lack or fail");
                expect(result).to.be.undefined;
                done();
            })
        });
        it('produce no key message is ok', done => {
            let topic = "test-topic";
            let message = {
                service: "service",
                type: "event",
                data: {
                    uid: "uid"
                }
            };
            messageProducer.produceMessage(topic, message, (err, result) => {
                expect(err).to.be.null;
                expect(result["test-topic"]).to.be.exist;
                expect(result["test-topic"]["0"]).to.be.exist;
                done();
            })
        });
        it('produce have key message is ok', done => {
            let topic = "test-key-topic";
            let message = {
                key: "uid",
                value: {
                    service: "service",
                    type: "event",
                    data: {
                        uid: "uid"
                    }
                }
            };
            messageProducer.produceMessage(topic, message, (err, result) => {
                expect(err).to.be.null;
                expect(result["test-key-topic"]).to.be.exist;
                expect(result["test-key-topic"]["0"]).to.be.exist;
            });
            kafkaConsumer.on('message', (message) => {
                if (message.key == "uid") {
                    let data = JSON.parse(message.value);
                    expect(data.service).to.equal("service");
                    expect(data.type).to.equal("event");
                    expect(data.data).to.be.exist;
                    done();
                }
            });
        });
    });
});
