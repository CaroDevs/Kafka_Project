const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kafka-node-consumer',
  brokers: ['172.24.172.4:9092']
});

const consumer = kafka.consumer({ groupId: 'itm-group-node' });

async function run() {
  try {
    await consumer.connect();
    console.log('==================================================');
    console.log(' [x] Consumidor de Node.js conectado. Esperando mensajes...');
    console.log('==================================================');

    await consumer.subscribe({ topic: 'quickstart-events-public', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const mensajeStr = message.value.toString();
        console.log(` [->] Recibido del tópico [${topic}]: ${mensajeStr}`);
      },
    });
  } catch (error) {
    console.error(' [X] Error en el consumidor:', error);
  }
}

run();