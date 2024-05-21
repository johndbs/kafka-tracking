package com.thinkitdevit.kafkatracking.handler;

import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.kafkatracking.service.TrackingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Component
public class DispatchPreparingHandler {

    private final TrackingService trackingService;

    @KafkaListener(id="dispatchPreparingConsumerClient",
            topics = "dispatch.tracking",
            groupId = "tracking.dispatch.tracking",
            containerFactory = "kafkaListenerContainerFactory" )
    public void listen(DispatchPreparing payload){
        try {
            trackingService.process(payload);
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    }

}
