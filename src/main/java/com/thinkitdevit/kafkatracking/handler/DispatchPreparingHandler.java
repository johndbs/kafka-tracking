package com.thinkitdevit.kafkatracking.handler;

import com.thinkitdevit.dispatch.message.DispatchCompleted;
import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.kafkatracking.service.TrackingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(id="dispatchPreparingConsumerClient",
        topics = "dispatch.tracking",
        groupId = "tracking.dispatch.tracking",
        containerFactory = "kafkaListenerContainerFactory" )
public class DispatchPreparingHandler {

    private final TrackingService trackingService;

    @KafkaHandler
    public void listen(DispatchPreparing payload){
        try {
            trackingService.processPreparing(payload);
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    }

    @KafkaHandler
    public void listen(DispatchCompleted payload){
        try {
            trackingService.processDispatchCompleted(payload);
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    }

    @KafkaHandler(isDefault = true)
    public void listen(Object payload){
        log.warn("Unknown message type: {}", payload);
    }

}
