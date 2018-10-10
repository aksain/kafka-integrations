package com.aksain.kafka.akka.consumers;

import akka.actor.ActorRef;
import com.aksain.kafka.akka.actors.KafkaResponseActor;
import com.aksain.kafka.akka.utils.ActorSystemUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Amit Kumar
 */
public class KafkaMessageConsumer extends Thread {
    public static final String RESPONSE_TOPIC_NAME = "response-topic";

    protected final String responseTopic;
    protected final KafkaConsumer<String, String> kafkaConsumer;

    public KafkaMessageConsumer(String topicName) {
        this.responseTopic = topicName;

        final Properties properties = new Properties();
        properties.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers", "localhost:9092"));
        properties.put("group.id", topicName);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "100");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(responseTopic));
    }

    @Override
    public void run() {
        while(!isInterrupted()) {
            final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            for(ConsumerRecord<String, String> record : consumerRecords.records(responseTopic)) {
                System.out.println("[Consumer]Received message with key: " + record.key());
                // Get actor with same name as message key and send message value to it
                ActorSystemUtils.getRootActor(record.key()).tell(
                        new KafkaResponseActor.KafkaResponse(record.value()),
                        ActorRef.noSender()
                );
            }
        }
    }
}
