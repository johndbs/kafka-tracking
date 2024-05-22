package com.thinkitdevit.kafkatracking.integration;


import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.dispatch.message.TrackingStatusUpdated;
import com.thinkitdevit.kafkatracking.config.KafkaTrackingConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(classes = {KafkaTrackingConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class KafkaTrackingIntegrationTest {

    public static final String TRACKING_STATUS_TOPIC = "tracking.status";
    public static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";


    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    private KafkaTestListener kafkaListener;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;


    @Configuration
    static class TestConfig {

        @Bean
        public KafkaTestListener kafkaListener(){
            return new KafkaTestListener();
        }

    }

    public static class KafkaTestListener{
        AtomicInteger trackingStatusCount = new AtomicInteger(0);
        AtomicInteger dispatchTrackingCount = new AtomicInteger(0);

        @KafkaListener(topics = TRACKING_STATUS_TOPIC, groupId = "kafkaTest")
        public void listenTrackingStatus(TrackingStatusUpdated payload){
            log.info("Received message: {}", payload);
            trackingStatusCount.incrementAndGet();
        }

        @KafkaListener(topics = DISPATCH_TRACKING_TOPIC, groupId = "kafkaTest")
        public void listenDispatchTracking(DispatchPreparing payload){
            log.info("Received message: {}", payload);
            dispatchTrackingCount.incrementAndGet();
        }

    }

    @BeforeEach
    public void setUp(){
        kafkaListener.dispatchTrackingCount.set(0);
        kafkaListener.trackingStatusCount.set(0);

        registry.getListenerContainers()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
    }


    @Test
    public void testTrackingStatusFlow() throws ExecutionException, InterruptedException {

        UUID orderId = UUID.randomUUID();
        DispatchPreparing dispatchPreparing = DispatchPreparing.builder()
                .orderId(orderId)
                .build();

        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchPreparing);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.dispatchTrackingCount::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.trackingStatusCount::get, equalTo(1));

    }

    private void sendMessage(String topic, Object payload) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }

}
