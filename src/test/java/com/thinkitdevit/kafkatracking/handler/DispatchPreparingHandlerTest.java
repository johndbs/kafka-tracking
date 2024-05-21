package com.thinkitdevit.kafkatracking.handler;

import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.kafkatracking.service.TrackingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

import static org.junit.jupiter.api.Assertions.*;

class DispatchPreparingHandlerTest {

    private TrackingService trackingService;

    private DispatchPreparingHandler dispatchPreparingHandler;

    @BeforeEach
    void setUp() {
        trackingService = mock(TrackingService.class);
        dispatchPreparingHandler = new DispatchPreparingHandler(trackingService);
    }

    @Test
    void listen_Success() throws ExecutionException, InterruptedException {

        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();

        dispatchPreparingHandler.listen(payload);

        verify(trackingService, times(1)).process(payload);
    }

    @Test
    void listen_ThrowsException() throws ExecutionException, InterruptedException {

        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();

        doThrow(new RuntimeException("Error processing message")).when(trackingService).process(payload);

        dispatchPreparingHandler.listen(payload);

        verify(trackingService, times(1)).process(payload);
    }

}