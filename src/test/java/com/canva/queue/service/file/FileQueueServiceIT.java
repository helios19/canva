package com.canva.queue.service.file;

import com.canva.queue.service.common.AbstractQueueServiceIT;
import com.canva.queue.service.memory.InMemoryQueueService;
import org.junit.Test;

/**
 * Integration test verifying the correct execution of the {@link FileQueueService} class while being invoked by
 * multiple concurrent producers consumers.
 */
public class FileQueueServiceIT extends AbstractQueueServiceIT {

    // Uncomment below annotation to run integration test
    //@Test
    public void shouldRunInConcurrencyModeWithMultipleProducersConsumers() throws Exception {

        String baseDirPath = null;
        Integer visibilityTimeoutInSecs = 2;
        boolean addShutdownHook = true;
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        FileQueueService queueService = new FileQueueService(baseDirPath, visibilityTimeoutInSecs, addShutdownHook);

        runQueueServiceWithMultipleProducersConsumers(queueUrl, queueService);

    }
}