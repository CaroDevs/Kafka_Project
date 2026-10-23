const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kafka-node-producer',
  brokers: ['172.24.172.4:9092'] // Si llegaras a tener problemas de red, puedes probar con 'localhost:9092'
});

const producer = kafka.producer();

async function run() {
  try {
    await producer.connect();
    console.log('==================================================');
    console.log(' [x] Productor de Node.js conectado a Kafka...');
    console.log('==================================================');

    const data = {
      usuario: "Diana",
      mensaje: "Ensayo",
      timestamp: Date.now()
    };

    await producer.send({
      topic: 'quickstart-events-public',
      messages: [
        { value: JSON.stringify(data) }
      ],
    });

    console.log(' [V] ¡Mensaje enviado con éxito a Kafka!', data);
  } catch (error) {
    console.error(' [X] Error al enviar el mensaje:', error);
  } finally {
    await producer.disconnect();
    console.log(' [x] Productor cerrado.');
  }
}

run().catch(console.error);