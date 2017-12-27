package com.aksain.kafka.akka.main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Amit Kumar
 */
public class KafkaMessageProducer {
    private static final KafkaMessageProducer INSTANCE = new KafkaMessageProducer();

    public static final String REQUEST_TOPIC_NAME = "request-topic";
    private final Producer<String, String> kafkaProducer;

    private KafkaMessageProducer() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers", "localhost:9092"));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<>(properties);
    }

    public static KafkaMessageProducer getInstance() {
        return INSTANCE;
    }

    public void sendMessage(String topicName, String key, String value) {
        kafkaProducer.send(new ProducerRecord<>(topicName, key, value));
    }
}
