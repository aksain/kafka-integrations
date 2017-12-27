package com.aksain.kafka.akka.processors;

import com.aksain.kafka.akka.consumers.KafkaMessageConsumer;
import com.aksain.kafka.akka.main.KafkaMessageProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author Amit Kumar
 */
public class KafkaMessageProcessor extends KafkaMessageConsumer {
    public KafkaMessageProcessor(String topicName) {
        super(topicName);
    }

    @Override
    public void run() {
        while(!isInterrupted()) {
            // Pull messages from request-topic and send to response-topic
            final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            for(ConsumerRecord<String, String> record : consumerRecords.records(responseTopic)) {
                System.out.println("[Processor]Received message with key: " + record.key());
                KafkaMessageProducer.getInstance().sendMessage(RESPONSE_TOPIC_NAME, record.key(), record.value());
            }
        }
    }
}
