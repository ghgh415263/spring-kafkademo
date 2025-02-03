package com.example.springkafkademo.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @KafkaListener 하나당 하나의 KafkaMessageListenerContainer가 생성됨.
 * 각 컨테이너는 독립적인 KafkaConsumer를 가짐.
 * concurrency 옵션을 사용하면 하나의 컨테이너에서 여러 개의 KafkaConsumer를 실행할 수 있음.
 */

@Component
public class ConsumerListener {

    /**
     * @KafkaListener 는 기본적으로 한 번의 poll()에서 여러 개의 메시지를 가져오지만, 하나씩 개별적으로 리스너를 호출하는 방식입니다.
     */
    @KafkaListener(topics = "dev_topic", groupId = "group5", concurrency = "1", errorHandler = "errorHandler")
    public void listener(String message) {
        System.out.println("Listener: " + message);
        // ack.mode=MANUAL 해도 배치 단위 커밋 (현재 poll()로 가져온 모든 메시지의 마지막 오프셋만 커밋됨)
    }

}
