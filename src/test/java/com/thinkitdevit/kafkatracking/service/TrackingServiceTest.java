package com.thinkitdevit.kafkatracking.service;

import com.thinkitdevit.dispatch.message.DispatchCompleted;
import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.dispatch.message.TrackingStatus;
import com.thinkitdevit.dispatch.message.TrackingStatusUpdated;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TrackingServiceTest {

    private KafkaTemplate<String, Object> kafkaProducer;

    private TrackingService trackingService;

    @BeforeEach
    void setUp() {
        kafkaProducer = mock(KafkaTemplate.class);
        trackingService = new TrackingService(kafkaProducer);
    }


    @Test
    void process_Preparing_Success() throws ExecutionException, InterruptedException {
        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();

        when(kafkaProducer.send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.PREPARING)
                .build())).thenReturn(mock(CompletableFuture.class));

        trackingService.processPreparing(payload);

        verify(kafkaProducer, times(1)).send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.PREPARING)
                .build());
    }

    @Test
    void process_Preparing_dispatchTrackingStatusFailed()  {
        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();

        when(kafkaProducer.send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.PREPARING)
                .build())).thenThrow(new RuntimeException("tracking.status failed"));

        Exception exception = assertThrows(RuntimeException.class, () ->trackingService.processPreparing(payload));

        verify(kafkaProducer, times(1)).send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.PREPARING)
                .build());
        assertThat(exception.getMessage()).isEqualTo("tracking.status failed");
    }

    @Test
    void process_DispatchCompleted_Success() throws ExecutionException, InterruptedException {
        DispatchCompleted payload = DispatchCompleted.builder().orderId(UUID.randomUUID())
                .distpatchedDate(LocalDate.now().toString()).build();

        when(kafkaProducer.send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.COMPLETED)
                .build())).thenReturn(mock(CompletableFuture.class));

        trackingService.processDispatchCompleted(payload);

        verify(kafkaProducer, times(1)).send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.COMPLETED)
                .build());
    }

    @Test
    void process_DispatchCompleted_dispatchTrackingStatusFailed()  {
        DispatchCompleted payload = DispatchCompleted.builder().orderId(UUID.randomUUID())
                .distpatchedDate(LocalDate.now().toString()).build();

        when(kafkaProducer.send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.COMPLETED)
                .build())).thenThrow(new RuntimeException("tracking.status failed"));

        Exception exception = assertThrows(RuntimeException.class, () ->trackingService.processDispatchCompleted(payload));

        verify(kafkaProducer, times(1)).send("tracking.status", TrackingStatusUpdated.builder()
                .orderId(payload.getOrderId())
                .status(TrackingStatus.COMPLETED)
                .build());
        assertThat(exception.getMessage()).isEqualTo("tracking.status failed");
    }


}