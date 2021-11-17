package com.spring.kafka.consumer;

import com.spring.kafka.domain.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Slf4j
@Component
public class KafkaSpringCloudMessageConsumer {
    @Bean
    Consumer<String> domainEventString() {
        return input -> {
            System.out.println("Received Message : " + input);
        };
    }

    @Bean
    Consumer<User> domainEventModel() {
        return input -> {
            System.out.println("Received Message : " + input);
            // 재시도 테스트를 위해 예외 처리 추가
            throw new UnknownError("unexpected error");
        };
    }
}
