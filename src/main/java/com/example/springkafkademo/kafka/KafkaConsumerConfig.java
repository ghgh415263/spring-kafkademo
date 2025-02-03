package com.example.springkafkademo.kafka;

/**
 * Container 역할
 *
 * 1. Consumer 생성 및 관리:
 * 컨테이너는 Kafka Consumer를 생성하고 이를 관리합니다.
 * 파티션 리밸런싱, 메시지 오프셋 관리 등을 자동으로 처리합니다.
 *
 * 2. 메시지 처리:
 * Kafka에서 메시지를 가져오면 지정된 MessageListener 또는 @KafkaListener 메서드를 호출하여 메시지를 처리합니다.
 *
 * 3.리밸런싱 관리:
 * Consumer 그룹 내에서 리밸런싱이 발생하면, 이를 처리하여 각 Consumer가 처리할 파티션을 재할당합니다.
 *
 * 4. 에러 처리 및 재시도:
 * 메시지를 처리하는 도중 발생하는 오류를 처리하고, 재시도 로직을 구현할 수 있습니다.
 *
 * 5.스레드 및 병렬 처리:
 * 여러 스레드를 사용하여 병렬로 메시지를 처리할 수 있으며, 컨커런시 설정을 통해 여러 Consumer가 동시에 메시지를 처리하도록 할 수 있습니다.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaMessageListenerContainer는 하나의 MessageListener만 설정할 수 있습니다.
 * 하나의 KafkaMessageListenerContainer는 하나의 Consumer에 대해 하나의 MessageListener만 처리합니다.
 * ConcurrentMessageListenerContainer는 여러 개의 KafkaMessageListenerContainer를 내부에 가질 수 있고 이를 통해서 병렬로 실행할 수 있습니다.
 *
 * 병렬 처리의 예시
 * 예를 들어, concurrency를 3으로 설정하면, ConcurrentMessageListenerContainer는 3개의 KafkaMessageListenerContainer를 생성하고, 각각의 컨테이너는 독립적인 KafkaConsumer를 사용하여 메시지를 소비합니다.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 시나리오 1: 파티션 수가 3개인 경우
     * concurrency = 3일 때, 3개의 consumer가 각각 1개의 파티션을 맡고 메시지를 처리합니다.
     * 파티션 0 → consumer 1
     * 파티션 1 → consumer 2
     * 파티션 2 → consumer 3
     *
     * 시나리오 2: 파티션 수가 6개인 경우
     * concurrency = 3일 때, 3개의 consumer가 2개씩 파티션을 맡아 처리합니다.
     * consumer 1 → 파티션 0, 파티션 1
     * consumer 2 → 파티션 2, 파티션 3
     * consumer 3 → 파티션 4, 파티션 5
     */

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

}
