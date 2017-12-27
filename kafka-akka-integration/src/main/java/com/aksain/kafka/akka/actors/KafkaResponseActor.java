package com.aksain.kafka.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;

/**
 * @author Amit Kumar
 */
public class KafkaResponseActor extends AbstractLoggingActor {
    /**
     * Plain model representing a request for Kafka message
     */
    public static class KafkaMessageRequest {
    }

    /**
     * Represents response message pulled from Kafka
     */
    public static class KafkaResponse {
        private String data;

        public KafkaResponse(String data) {
            this.data = data;
        }
    }

    /**
     * Defines the behaviours of inout requests.
     *
     * @return Receive object containing request and behaviour mapping
     */
    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(KafkaMessageRequest.class, this::onKafkaMessageRequest)
                .build();
    }

    /**
     * Switches the context, defines behaviour and wait for KafkaResponse request
     *
     * @param kafkaMessageRequest represents request for Kafka Message
     */
    private void onKafkaMessageRequest(KafkaMessageRequest kafkaMessageRequest) {
        // Store reference of KafkaMessageRequest sender so that we can send message later
        final ActorRef senderRef = getSender();
        getContext().become(
                receiveBuilder()
                        .match(KafkaResponse.class, kafkaResponse -> {
                            // Use stored sender reference to send Kafka response message. Calling getSender() here
                            // will instead return thread that pulled message from Kafka
                            senderRef.tell(kafkaResponse.data, getSelf());
                            // Destroy itself as job is done
                            getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
                        })
                        .build()
        );
    }


    public static Props props() {
        return Props.create(KafkaResponseActor.class);
    }
}