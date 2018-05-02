'use strict';
const _ = require('underscore');
const kafka = require('kafka-node');
const expect = require('chai').expect;
const MessageConsumer = require('../../lib/message/messageConsumer');

describe('MessageConsumer(options) use case test', () => {
    let messageConsumer;
    let kafkaClient;
    let kafkaProducer;
    before(done => {
        let options = {};
        messageConsumer = new MessageConsumer(options);
        kafkaClient = new kafka.KafkaClient({
            kafkaHost: "127.0.0.1:9092",
            autoConnect: true
        });
        kafkaProducer = new kafka.HighLevelProducer(kafkaClient, {
            requireAcks: 1
        });
        kafkaProducer.on('ready', () => {
            done();
        });
        kafkaProducer.on('error', err => {
            kafkaClient.close();
            done(err);
        });
    });
    after(done => {
        kafkaClient.close();
        messageConsumer.close(false, done);
    });
    describe('#consumeMessage(topics, callback)', () => {
        it('is return a err if topics is lack or fail', done => {
            let topics = null;
            messageConsumer.consumeMessage(topics, (err, result) => {
                expect(err.message).to.equal("topics is lack or fail");
                expect(result).to.be.undefined;
                done();
            })
        });
        it('is ok', done => {
            let topics = ["test-topic3"];
            messageConsumer.consumeMessage(topics, (err, message) => {
                expect(err).to.be.null;
                expect(JSON.parse(message.value).service).to.equal('service');
                setTimeout(() => {
                    messageConsumer.commit((err, result) => {
                        done();
                    });
                }, 0);
            });
            let message = {
                service: "service",
                type: "event",
                timestamp: "12334235256545",
                payload: {
                    uid: "uid"
                }
            };
            kafkaProducer.send([
                {topic: 'test-topic3', messages: JSON.stringify(message)}
            ], () => {
            });
        });
    });
});
