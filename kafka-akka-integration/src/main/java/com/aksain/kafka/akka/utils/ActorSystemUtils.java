package com.aksain.kafka.akka.utils;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;

/**
 * Simple utility class to encapsulate ActorSystem and ActorSelection.
 *
 * @author Amit Kumar
 */
public class ActorSystemUtils {
    private static final ActorSystem ACTOR_SYSTEM = ActorSystem.create("kafka-request-response");
    private ActorSystemUtils() {}

    public static ActorSystem getActorSystem() {
        return ACTOR_SYSTEM;
    }

    public static ActorSelection getRootActor(String rootActorName) {
        return ACTOR_SYSTEM.actorSelection("/user/" + rootActorName);
    }
}
