/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
var avro = require("avsc");

var Transform = require('stream').Transform;

var Kafka = require('node-rdkafka');

const type = avro.Type.forSchema({
    name: 'Cliente',
    type: 'record',
    fields: [
      {name: 'id', type: 'int' },
      {name: 'nome', type: ['null', 'string'] }
    ]
  });

var consumer = new Kafka.KafkaConsumer({
  'metadata.broker.list': '10.2.157.115:9092',
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
  //'security.protocol':'SASL_PLAINTEXT',
  //'sasl.mechanism':'PLAIN',
  //'sasl.username':'admin',
  //'sasl.password':'admin-password',
  });
  
var topicName = 'laurowag';

var counter = 0;
var numMessages = 5;

consumer.on('ready', function(arg) {
    console.log('consumer ready.' + JSON.stringify(arg));
  
    consumer.subscribe([topicName]);
    //start consuming messages
    consumer.consume();
  });

consumer.on('data', function(m) {
    var decodedMessage = type.decode(m.value); // Skip prefix.
    console.log(decodedMessage);
});

consumer.on('error', function(err) {
    console.error('Error from consumer');
    console.error(err);
  });

//starting the consumer
consumer.connect();

//stopping this example after 30s
setTimeout(function() {
  consumer.disconnect();
}, 60000);