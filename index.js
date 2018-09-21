/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
var avro = require("avsc");
var axios = require('axios');

var Transform = require('stream').Transform;

var Kafka = require('node-rdkafka');

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
  
var topics = ['plataforma.mqtt','laurowag'];

var counter = 0;
var numMessages = 5;

const axiosInstance = axios.create({
  baseURL: '',
  timeout: 5000,
});

consumer.on('ready', function(arg) {
    console.log('consumer ready.' + JSON.stringify(arg));
    consumer.subscribe(topics);
    //start consuming messages
    consumer.consume();
  });

consumer.on('data', function(m) {
    var schemaId = m.value[4];
    var schema = undefined;

    axiosInstance.get('http://10.2.141.98:8081/schemas/ids/'+schemaId)
    .then(response => {
      var schemaType = avro.Type.forSchema(JSON.parse(response.data.schema));
      var decodedMessage = schemaType.decode(m.value, 5); // Skip schema identification.
      console.log(decodedMessage);
    })
    .catch(error => {      
      console.log(error);
    });

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