package com.thinkitdevit.kafkatracking.handler;

import com.thinkitdevit.dispatch.message.DispatchCompleted;
import com.thinkitdevit.dispatch.message.DispatchPreparing;
import com.thinkitdevit.kafkatracking.service.TrackingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

class DispatchPreparingHandlerTest {

    private TrackingService trackingService;

    private DispatchPreparingHandler dispatchPreparingHandler;

    @BeforeEach
    void setUp() {
        trackingService = mock(TrackingService.class);
        dispatchPreparingHandler = new DispatchPreparingHandler(trackingService);
    }

    @Test
    void listen_dispatchPreparing_Success() throws ExecutionException, InterruptedException {

        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();

        dispatchPreparingHandler.listen(payload);

        verify(trackingService, times(1)).processPreparing(payload);
    }

    @Test
    void listen_dispatchPreparing_ThrowsException() throws ExecutionException, InterruptedException {

        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();

        doThrow(new RuntimeException("Error processing message")).when(trackingService).processPreparing(payload);

        dispatchPreparingHandler.listen(payload);

        verify(trackingService, times(1)).processPreparing(payload);
    }

    @Test
    void listen_dispatchCompleted_Success() throws ExecutionException, InterruptedException {

        DispatchCompleted payload = DispatchCompleted.builder().orderId(UUID.randomUUID())
                .distpatchedDate(LocalDate.now().toString()).build();

        dispatchPreparingHandler.listen(payload);

        verify(trackingService, times(1)).processDispatchCompleted(payload);
    }

    @Test
    void listen_dispatchCompleted_ThrowsException() throws ExecutionException, InterruptedException {

        DispatchCompleted payload = DispatchCompleted.builder().orderId(UUID.randomUUID())
                .distpatchedDate(LocalDate.now().toString()).build();

        doThrow(new RuntimeException("Error processing message")).when(trackingService).processDispatchCompleted(payload);

        dispatchPreparingHandler.listen(payload);

        verify(trackingService, times(1)).processDispatchCompleted(payload);
    }

}