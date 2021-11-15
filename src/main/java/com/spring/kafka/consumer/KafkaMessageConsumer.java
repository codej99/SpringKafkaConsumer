package com.spring.kafka.consumer;

import com.spring.kafka.domain.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaMessageConsumer {
    @KafkaListener(topics = "domain-event")
    public void consumeMessage(@Headers MessageHeaders headers, @Payload String message) {
        log.debug("Received Headers : " + headers);
        log.debug("Received Payloads : " + message);
    }

    @KafkaListener(topics = "domain-event-user")
    public void listenDomainEvent(@Headers MessageHeaders headers, @Payload User user) {
        log.debug("Received Headers : " + headers);
        log.debug("Received Payloads : " + user.toString());
    }
}
