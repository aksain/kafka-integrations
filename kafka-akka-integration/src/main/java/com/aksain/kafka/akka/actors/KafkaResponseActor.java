package com.aksain.kafka.akka.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;

public class KafkaResponseActor extends AbstractLoggingActor {

    public static class KafkaMessageRequest {}

    public static class KafkaResponse {
        private String data;
        public KafkaResponse(String data) {
            this.data = data;
        }
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(KafkaMessageRequest.class, this::onKafkaMessageRequest)
                .build();
    }

    private void onKafkaMessageRequest(KafkaMessageRequest kafkaMessageRequest) {
        final ActorRef senderRef = getSender();
        getContext().become(
                receiveBuilder()
                        .match(KafkaResponse.class, kafkaResponse -> {
                            senderRef.tell(kafkaResponse.data, getSelf());
                            getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
                        })
                        .build()
        );
    }

    public static Props props() {
        return Props.create(KafkaResponseActor.class);
    }
}