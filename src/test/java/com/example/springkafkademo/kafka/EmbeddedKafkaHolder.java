package com.example.springkafkademo.kafka;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

public final class EmbeddedKafkaHolder {

    private static EmbeddedKafkaBroker embeddedKafka = new EmbeddedKafkaBroker(1);

    private static boolean started;

    public static EmbeddedKafkaBroker getEmbeddedKafka() {
        if (!started) {
            try {
                embeddedKafka.afterPropertiesSet();
            }
            catch (Exception e) {
                throw new KafkaException("Embedded broker failed to start", e);
            }
            started = true;
        }
        embeddedKafka.addTopics("dev_topic");
        return embeddedKafka;
    }

    private EmbeddedKafkaHolder() {
        super();
    }

}
