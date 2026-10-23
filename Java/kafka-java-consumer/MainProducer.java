package com.consumidor.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MainProducer {
    public static void main(String[] args) {
        // 1. Configuración del Productor de Kafka (Apuntando a la IP de la VM)
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.24.172.4:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topicName = "quickstart-events-public";

        // 2. El mensaje que quieres enviar desde Java hacia Kafka
        String mensaje = "Quiero ensayar algo más!";
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, mensaje);

        System.out.println("==================================================");
        System.out.println(" [x] Intentando enviar mensaje a Kafka...");
        System.out.println("==================================================");

        try {
            // Enviar el mensaje al servidor de Kafka
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(" [V] ¡Mensaje enviado con éxito a Kafka!");
                    System.out.println("     - Tópico: " + metadata.topic());
                    System.out.println("     - Partición: " + metadata.partition());
                    System.out.println("     - Offset: " + metadata.offset());
                } else {
                    System.err.println(" [X] Error al enviar el mensaje: " + exception.getMessage());
                }
            });

            // Forzar el envío inmediato
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Cerrar el productor
            producer.close();
            System.out.println(" [x] Productor cerrado.");
        }
    }
}