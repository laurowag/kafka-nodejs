import kafka from "kafka-node";
import avro from "avsc"
 
const client = new kafka.Client("localhost:2181", "my-client-id", {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
});
 
const consumer = new kafka.Consumer(client, 
    [
        { topic: 'test', partition: 0 }
    ],
    {
        autoCommit: false,
        encoding:'buffer'
    }
);

const type = avro.Type.forSchema({
    name: 'Cliente',
    type: 'record',
    fields: [
      {name: 'codigo', type: ['null', 'long'] },
      {name: 'nome', type: ['null', 'string'] }
    ]
  });

consumer.on('message', function (message) {
    try {
        //var buf = new Buffer(message.value, 'binary'); // Read string into a buffer.
        var decodedMessage = type.fromBuffer(message.value); // Skip prefix.
        console.log(decodedMessage);
    } catch (error) {
        console.log("error: "+error.message);
    }
});
 
// For this demo we just log producer errors to the console.
consumer.on("error", function(error) {
    console.error(error);
});