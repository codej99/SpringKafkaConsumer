package com.spring.kafka.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Configuration
public class KafkaConfiguration {

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        // 재시도시 1초 후에 재 시도하도록 backoff delay 시간을 설정한다.
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000L);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        // 최대 재시도 횟수 설정
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(2);
        retryTemplate.setRetryPolicy(retryPolicy);
        return retryTemplate;
    }

    private RecoveryCallback<Object> recoveryCallback(KafkaTemplate<String, String> kafkaTemplate) {
        return context -> {
            final var retryCount = context.getRetryCount();
            final var acknowledgment = (Acknowledgment) context.getAttribute("acknowledgment");
            final var record = (ConsumerRecord) context.getAttribute("record");
            final var topic = "dlt_" + record.topic();
            final var value = record.value().toString();
            try {
                log.warn("[Send to dead letter topic] {} - retryCount: {} - value: {}.", topic, retryCount, value);
                kafkaTemplate.send(topic, value);
            } catch (Exception e) {
                log.error("[Fail to dead letter topic]: {}" , topic, e);
            }
            if (Objects.nonNull(acknowledgment)) {
                acknowledgment.acknowledge();
            }
            return Optional.empty();
        };
    }

    private ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
        final var props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getConsumer().getAutoOffsetReset(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, KafkaProperties.IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory(KafkaProperties kafkaProperties, KafkaTemplate<String, String> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(kafkaProperties));
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(recoveryCallback(kafkaTemplate));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
