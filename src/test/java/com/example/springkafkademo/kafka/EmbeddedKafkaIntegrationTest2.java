package com.example.springkafkademo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Slf4j
@SpringJUnitConfig(EmbeddedKafkaIntegrationTest2.Config.class)
public class EmbeddedKafkaIntegrationTest2 {

    private static final EmbeddedKafkaBroker kafkaBroker = EmbeddedKafkaHolder.getEmbeddedKafka();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @SpyBean //이미 존재하는 Bean을 SpyBean으로 Wrapping한 형태
    private ConsumerListener consumerListener;

    @Captor
    ArgumentCaptor<String> messageCaptor;

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
    void kafkaTemplate로_embeddedKafka에_데이터보내기() throws ExecutionException, InterruptedException {
        ListenableFuture future = kafkaTemplate.send("dev_topic", "asd");
        future.get();

        verify(consumerListener, timeout(5000).times(1))
                .listener(messageCaptor.capture());

        assertEquals("asd", messageCaptor.getValue());
    }

}
