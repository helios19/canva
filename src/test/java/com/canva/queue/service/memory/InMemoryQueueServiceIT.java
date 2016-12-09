package com.canva.queue.service.memory;

import com.canva.queue.service.common.AbstractQueueServiceIT;
import org.junit.Test;

/**
 * Integration test verifying the correct execution of the {@link InMemoryQueueService} class while being invoked by
 * multiple concurrent producers consumers.
 */
public class InMemoryQueueServiceIT extends AbstractQueueServiceIT {

    // Uncomment below annotation to run integration test
    //@Test
    public void shouldRunInConcurrencyModeWithMultipleProducersConsumers() throws Exception {

        Integer visibilityTimeoutInSecs = 2;
        boolean runVisibilityCollector = true;
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        InMemoryQueueService queueService = new InMemoryQueueService(visibilityTimeoutInSecs, runVisibilityCollector);

        runQueueServiceWithMultipleProducersConsumers(queueUrl, queueService);

    }
}