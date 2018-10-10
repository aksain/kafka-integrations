package com.aksain.kafka.akka.main;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.aksain.kafka.akka.actors.KafkaResponseActor;
import com.aksain.kafka.akka.consumers.KafkaMessageConsumer;
import com.aksain.kafka.akka.processors.KafkaMessageProcessor;
import com.aksain.kafka.akka.utils.ActorSystemUtils;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.aksain.kafka.akka.actors.KafkaResponseActor.KafkaMessageRequest;

/**
 * @author Amit Kumar
 */
public class KafkaRequestResponseDemo {
    public static void main(String[] args) throws TimeoutException {
        // Start Kafka consumer to listen for messages from response queue
        final KafkaMessageConsumer kafkaMessageConsumer = new KafkaMessageConsumer(KafkaMessageConsumer.RESPONSE_TOPIC_NAME);
        kafkaMessageConsumer.start();

        // Start Message processor for picking up messages from request topic to put them into response topic
        final KafkaMessageProcessor kafkaMessageProcessor = new KafkaMessageProcessor(KafkaMessageProducer.REQUEST_TOPIC_NAME);
        kafkaMessageProcessor.start();

        final String customerId = "123";
        final String actorName = "customer_" + customerId + "_" + UUID.randomUUID();

        // Get Akka Actor System
        final ActorSystem system = ActorSystemUtils.getActorSystem();
        // Create an actor with name derived from customerId and a random and unique id
        final ActorRef kafkaReqResActorRef = system.actorOf(KafkaResponseActor.props(), actorName);

        // Create an inbox to communicate with Actor
        final Inbox inbox = Inbox.create(system);
        inbox.send(kafkaReqResActorRef, new KafkaMessageRequest());

        // Send message to Kafka for processing
        final String messageKey = actorName;
        final String message = "Test Response Data";
        KafkaMessageProducer.getInstance().sendMessage(KafkaMessageProducer.REQUEST_TOPIC_NAME, messageKey, message);

        // Wait for Response message from Actor with a configured timeout
        final long messageWaitTimeout = Long.valueOf(System.getProperty("message.wait.timeout", "3000"));
        system.log().info("Message Found: " + inbox.receive(Duration.create(messageWaitTimeout, TimeUnit.MILLISECONDS)).equals(message));

        // Shutdown Akka Actor System
        system.terminate();
    }
}