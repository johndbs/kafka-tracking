package com.thinkitdevit.kafkatracking.service;

import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.dispatch.message.TrackingStatus;
import com.thinkitdevit.dispatch.message.TrackingStatusUpdated;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Service
public class TrackingService {

    public static final String TRACKING_STATUS_TOPIC = "tracking.status";
    private final KafkaTemplate<String, Object> kafkaProducer;

    public void process(DispatchPreparing dispatchPreparing) throws ExecutionException, InterruptedException {
        log.info("Received message: {}", dispatchPreparing);

        TrackingStatusUpdated trackingStatusUpdated = TrackingStatusUpdated.builder()
                .orderId(dispatchPreparing.getOrderId())
                .status(TrackingStatus.PREPARING)
                .build();

        kafkaProducer.send(TRACKING_STATUS_TOPIC, trackingStatusUpdated).get();
    }

}
