package com.consumidor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

public class MainConsumer {
    public static void main(String[] args) {
        // Configuración idéntica a la del script de Python de tu profesor
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.24.172.4:9092");
        props.put("group.id", "itm-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String topicName = "quickstart-events-public";
        consumer.subscribe(Collections.singletonList(topicName));

        System.out.println("==================================================");
        System.out.println(" [x] Consumidor Java en MODO ENSAYO (Sin MySQL)");
        System.out.println(" [x] Conectado a Kafka: 172.24.172.4:9092");
        System.out.println(" [x] Escuchando tópico: " + topicName);
        System.out.println("==================================================");

        try {
            while (true) {
                // Escucha continua de mensajes (Polling)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String mensaje = record.value();
                    System.out.println("\n--------------------------------------------------");
                    System.out.println(" [->] Mensaje recibido de Kafka: " + mensaje);
                    System.out.println(" [V] [SIMULACIÓN] Procesado correctamente a las " + LocalDateTime.now());
                    System.out.println("--------------------------------------------------");
                }
            }
        } catch (Exception e) {
            System.err.println(" [X] Error en el consumidor: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}