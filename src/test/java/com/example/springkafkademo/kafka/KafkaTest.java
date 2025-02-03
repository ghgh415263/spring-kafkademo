package com.example.springkafkademo.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class KafkaTest {

    private static final EmbeddedKafkaBroker kafkaBroker = EmbeddedKafkaHolder.getEmbeddedKafka();

    private final static String TEST_TOPIC = "test-topic";

    @Test
    public void test() {
        // 토픽 추가
        kafkaBroker.addTopics(TEST_TOPIC);
        sendTestMessages(kafkaBroker);

        // consumer 생성, 100개씩 가져오게
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", kafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<String, String> consumer = cf.createConsumer();
        kafkaBroker.consumeFromAnEmbeddedTopic(consumer, TEST_TOPIC);
        ConsumerRecords<String, String> replies = KafkaTestUtils.getRecords(consumer);
        consumer.close();

        assertThat(replies.count()).isEqualTo(100);
    }

    private void sendTestMessages(EmbeddedKafkaBroker broker) {
        String topic = TEST_TOPIC;
        String message = "Test message";

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", broker.getBrokersAsString());
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i=0; i<200; i++){
                producer.send(new ProducerRecord<>(topic, message+i));
            }
        }
    }

}
