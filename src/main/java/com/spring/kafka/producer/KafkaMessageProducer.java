package com.spring.kafka.producer;

import com.spring.kafka.domain.model.User;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@RequiredArgsConstructor
public class KafkaMessageProducer {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    private final StreamBridge streamBridge;

    public void sendMessage(String payload) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> message = new ProducerRecord<>("domain-event", payload);
        producer.send(message);
    }

    public void sendMessage(User user) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, User> message = new ProducerRecord<>("domain-event-user", user);
        producer.send(message);
    }

    public void sendMessageBySpringCloud(String payload) {
        streamBridge.send("domainEventString-out-0", payload);
    }

    public void sendMessageBySpringCloud(User payload) {
        streamBridge.send("domainEventModel-out-0", payload);
    }
}
