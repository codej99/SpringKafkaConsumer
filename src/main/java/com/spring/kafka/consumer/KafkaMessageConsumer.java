package com.spring.kafka.consumer;

import com.spring.kafka.domain.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageConsumer {
    @KafkaListener(topics = "domain-event")
    public void consumeMessage(@Headers MessageHeaders headers, @Payload String message) {
        System.out.println("Received Headers : "+headers);
        System.out.println("Received Payloads : "+message);
    }

    @KafkaListener(topics = "domain-event-user")
    public void listenDomainEvent(@Headers MessageHeaders headers, @Payload User user) {
        System.out.println("Received Headers : "+headers);
        System.out.println("Received Payloads : "+user.toString());
    }
}
