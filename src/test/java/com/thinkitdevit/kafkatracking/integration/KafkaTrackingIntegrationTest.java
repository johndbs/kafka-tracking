package com.thinkitdevit.kafkatracking.integration;


import com.thinkitdevit.dispatch.message.DispatchCompleted;
import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.dispatch.message.TrackingStatusUpdated;
import com.thinkitdevit.kafkatracking.config.KafkaTrackingConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(classes = {KafkaTrackingConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
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
        AtomicInteger dispatchTrackingPreparingCount = new AtomicInteger(0);
        AtomicInteger dispatchTrackingCompletedCount = new AtomicInteger(0);

        @KafkaListener(topics = TRACKING_STATUS_TOPIC, groupId = "kafkaTest")
        public void listenTrackingStatus(@Payload TrackingStatusUpdated payload){
            log.info("Received message: {}", payload);
            trackingStatusCount.incrementAndGet();
        }

        @KafkaListener(topics = DISPATCH_TRACKING_TOPIC, groupId = "kafkaTest")
        public void listenDispatchTracking( ConsumerRecord consumerRecord){
            log.info("Received message: {}", consumerRecord);

            Object payload = consumerRecord.value();
            
            if(payload instanceof DispatchPreparing){
                dispatchTrackingPreparingCount.incrementAndGet();
            }else if (payload instanceof DispatchCompleted) {
                dispatchTrackingCompletedCount.incrementAndGet();
            }else{
                log.warn("Unknown message type: {}", payload);
                throw new IllegalArgumentException("Unknown message type");
            }

        }

    }

    @BeforeEach
    public void setUp(){
        kafkaListener.dispatchTrackingPreparingCount.set(0);
        kafkaListener.dispatchTrackingCompletedCount.set(0);
        kafkaListener.trackingStatusCount.set(0);

        registry.getListenerContainers()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container,
                        container.getContainerProperties().getTopics().length * embeddedKafkaBroker.getPartitionsPerTopic()));
    }


    @Test
    public void testDispatchPreparingTrackingStatusFlow() throws ExecutionException, InterruptedException {

        UUID orderId = UUID.randomUUID();
        DispatchPreparing dispatchPreparing = DispatchPreparing.builder()
                .orderId(orderId)
                .build();

        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchPreparing);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.dispatchTrackingPreparingCount::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.dispatchTrackingCompletedCount::get, equalTo(0));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.trackingStatusCount::get, equalTo(1));

    }

    @Test
    public void testDispatchCompletedTrackingStatusFlow() throws ExecutionException, InterruptedException {

        UUID orderId = UUID.randomUUID();
        DispatchCompleted dispatchCompleted = DispatchCompleted.builder()
                .orderId(orderId)
                .distpatchedDate(LocalDate.now().toString())
                .build();

        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchCompleted);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.dispatchTrackingPreparingCount::get, equalTo(0));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.dispatchTrackingCompletedCount::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaListener.trackingStatusCount::get, equalTo(1));

    }

    private void sendMessage(String topic, Object payload) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }

}
