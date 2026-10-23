const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise');


const dbConfig = {
  host: '127.0.0.1',
  user: 'root',
  password: '', 
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

const DB_NAME = 'kafka-Andres-Diana';
const TABLE_NAME = 'logs';

async function inicializarBaseDeDatos() {
  try {
      const connection = await mysql.createConnection(dbConfig);
    
    await connection.query(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` DEFAULT CHARACTER SET 'utf8mb4'`);
    console.log(` [V] Base de datos '${DB_NAME}' verificada/creada correctamente.`);
    
    await connection.end();
    const db = await mysql.createConnection({ ...dbConfig, database: DB_NAME });
    
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS \`${TABLE_NAME}\` (
        \`id\` INT AUTO_INCREMENT PRIMARY KEY,
        \`mensaje\` TEXT NOT NULL,
        \`fecha\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      ) ENGINE=InnoDB;
    `;
    await db.query(createTableQuery);
    console.log(` [V] Tabla '${TABLE_NAME}' verificada/creada correctamente.`);
    await db.end();
  } catch (error) {
    console.error(' [X] Error configurando la base de datos:', error.message);
    process.exit(1);
  }
}

async function run() {
  
  await inicializarBaseDeDatos();
  const db = await mysql.createConnection({ ...dbConfig, database: DB_NAME });
  const kafka = new Kafka({
    clientId: 'kafka-node-db-consumer',
    brokers: ['172.24.172.4:9092']
  });

  const consumer = kafka.consumer({ groupId: 'itm-group' });

  try {
    await consumer.connect();
    console.log('==================================================');
    console.log(' [x] Consumidor Node.js conectado a Kafka.');
    console.log(' [x] Escuchando tópico: quickstart-events-public');
    console.log('==================================================');

    await consumer.subscribe({ topic: 'quickstart-events-public', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const mensajeStr = message.value.toString();
        console.log(` [->] Recibido de Kafka: ${mensajeStr}`);

        const insertSql = `INSERT INTO \`${TABLE_NAME}\` (\`mensaje\`) VALUES (?)`;
        try {
          await db.execute(insertSql, [mensajeStr]);
          console.log(' [V] Log almacenado exitosamente en MySQL.');
        } catch (dbError) {
          console.error(' [X] Error insertando en la base de datos:', dbError.message);
        }
      },
    });
  } catch (error) {
    console.error(' [X] Error en el consumidor de Kafka:', error);
  }
}

run().catch(console.error);