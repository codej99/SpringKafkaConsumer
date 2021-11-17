package com.spring.kafka.controller;

import com.spring.kafka.domain.model.User;
import com.spring.kafka.producer.KafkaMessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MessageSendController {

    private final KafkaMessageProducer producer;

    /*
    curl --location --request POST 'http://localhost:8080/send-message' \
    --header 'Content-Type: text/plain' \
    --data-raw 'message'
     */
    @PostMapping("/send-message")
    public void sendMessage(@RequestBody String payload) {
        producer.sendMessage(payload);
    }

    /*
    curl --location --request POST 'http://localhost:8080/send-message-user' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "id": "happydaddy@naver.com",
        "name": "happydaddy",
        "age": 28
    }'
     */
    @PostMapping("/send-message-user")
    public void sendMessage(@RequestBody User payload) {
        producer.sendMessage(payload);
    }

    /*
    curl --location --request POST 'http://localhost:8080/send-message-by-spring-cloud' \
        --header 'Content-Type: text/plain' \
        --data-raw 'message'
     */
    @PostMapping("/send-message-by-spring-cloud")
    public void sendMessageBySpringCloud(@RequestBody String payload) {
        producer.sendMessageBySpringCloud(payload);
    }

    /*
    curl --location --request POST 'http://localhost:8080/send-message-by-spring-cloud-user' \
        --header 'Content-Type: application/json' \
        --data-raw '{
            "id": "happydaddy@naver.com",
            "name": "happydaddy",
            "age": 28
        }'
     */
    @PostMapping("/send-message-by-spring-cloud-user")
    public void sendMessageBySpringCloud(@RequestBody User payload) {
        producer.sendMessageBySpringCloud(payload);
    }
}
