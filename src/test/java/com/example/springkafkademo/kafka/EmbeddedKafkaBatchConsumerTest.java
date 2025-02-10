package com.example.springkafkademo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@SpringJUnitConfig(EmbeddedKafkaBatchConsumerTest.Config.class)
public class EmbeddedKafkaBatchConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final EmbeddedKafkaBroker kafkaBroker = EmbeddedKafkaHolder.getEmbeddedKafka();
    @EnableKafka
    @Configuration
    static class Config {

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfigs());
        }

        @Bean
        public Map<String, Object> producerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return props;
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
            factory.setRecordFilterStrategy(consumerRecord ->
                    true
            );
            factory.setCommonErrorHandler(new DefaultErrorHandler(
                    (data, thrownException) -> {
                        System.out.println("Common ErrorHandler: " + thrownException.getMessage());
                    }
            ));
            return factory;
        }

        @Bean
        public KafkaListenerErrorHandler errorHandler() {
            return (message, exception) -> {
                System.out.println("Error occurred while processing message: " + exception.getMessage());
                return null; // 예외를 처리한 후 반환값을 설정 (null로 반환하거나 특정 객체 반환)
            };
        }

        @Bean
        public ConsumerListener consumerListener(){
            return new ConsumerListener();
        }
    }


    @Test
    void asdasd() throws InterruptedException {
        for (int i=0; i < 10000; i++)
            kafkaTemplate.send("dev_topic", "asd");

        Thread.sleep(2000);

        kafkaBroker.doWithAdmin(admin -> {
            try {
                Map<TopicPartition, OffsetAndMetadata> map = admin.listConsumerGroupOffsets("group5").partitionsToOffsetAndMetadata().get();
                System.out.println("filter되도 커밋됨?: " + map);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
