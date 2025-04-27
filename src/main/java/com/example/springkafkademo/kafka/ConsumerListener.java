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

    /*
    RECORD: 리스너가 레코드 처리를 마친 후 오프셋을 커밋합니다.

    BATCH: poll()에 의해 반환된 모든 레코드를 처리한 후 오프셋을 커밋합니다.

    TIME: poll()에 의해 반환된 모든 레코드를 처리한 후, 마지막 커밋 이후 ackTime이 초과되면 오프셋을 커밋합니다.

    COUNT: poll()에 의해 반환된 모든 레코드를 처리한 후, 마지막 커밋 이후 ackCount만큼 레코드가 수신되면 오프셋을 커밋합니다.

    COUNT_TIME: TIME과 COUNT와 유사하지만, 조건 중 하나라도 충족되면 커밋이 수행됩니다.

    MANUAL: 메시지 리스너가 acknowledge()를 호출하여 오프셋을 커밋합니다. 그 후 BATCH와 동일한 동작이 적용됩니다.

    MANUAL_IMMEDIATE: 리스너가 Acknowledgment.acknowledge() 메서드를 호출하면 즉시 오프셋을 커밋합니다. */

}
