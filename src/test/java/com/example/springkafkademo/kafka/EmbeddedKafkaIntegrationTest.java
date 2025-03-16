package com.example.springkafkademo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/*
* 스프링 테스트에서는 같은 Context를 사용하는 테스트가 존재할 때, 기존의 Context를 재활용한다(ContextCaching)
* 따라서 애플리케이션 컨텍스트의 구성이나 상태를 테스트 내에서 변경하지 않아야 한다.
* 그렇지 않다면 테스트간 격리가 수행되지 않을 수 있다(변경된 컨텍스트를 다른 테스트에서 사용하는 문제).
* 이 때, @DirtiesContext 가 해결책이 될 수 있다.
* @DirtiesContext는 각 테스트를 수행할 때마다 context를 생성 가능하도록 한다.
*
* */
@Slf4j
@SpringJUnitConfig(EmbeddedKafkaIntegrationTest.Config.class)
@EmbeddedKafka(partitions = 1, topics = {"test-topic"}, brokerProperties = { "listeners=PLAINTEXT://localhost:9092"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class EmbeddedKafkaIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @Configuration
    static class Config {

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfigs());
        }

        @Bean
        public Map<String, Object> producerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            return new DefaultKafkaConsumerFactory<>(props);
        }
    }

    @Test
    void kafkaTemplate로_embeddedKafka에_데이터보내기() throws ExecutionException, InterruptedException {
        kafkaTemplate.send("test-topic", "asd").get(); // 비동기로 전송하기 떄문에 get 호출

        Consumer<String, String> kafkaConsumer = new KafkaConsumer<>(KafkaTestUtils.consumerProps("testConsumer", "false", kafkaBroker));
        kafkaConsumer.subscribe(Arrays.asList("test-topic"));

        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(kafkaConsumer);

        assertThat(records.count()).isEqualTo(1);
        kafkaConsumer.close();
    }

}
