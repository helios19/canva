package com.canva.queue.service.common;

import com.canva.queue.message.MessageQueue;
import com.canva.queue.service.QueueService;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract integration test class declaring method to run {@link com.canva.queue.service.QueueService}
 * implementation class with multiple producers consumers running concurrently.
 */
public abstract class AbstractQueueServiceIT {

    private static final Log LOG = LogFactory.getLog(AbstractQueueServiceIT.class);

    protected void runQueueServiceWithMultipleProducersConsumers(String queueUrl, QueueService queueService) throws InterruptedException, ExecutionException {
        // ------------------------------------------------------------------------ //
        // 1- Run multiple concurrent producers simultaneously to fill up the queue //
        // ------------------------------------------------------------------------ //

        // atomic counter for creating new message body
        final AtomicInteger counter = new AtomicInteger(0);

        // pushers list
        List<Callable<String>> pushers = Lists.newArrayList();

        // executor instance that run thread in parallel
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (int i = 0; i < 100; i++) {
            Callable worker = () -> {

                String messageBody = "message" + counter.incrementAndGet();

                queueService.push(queueUrl, null, messageBody);

                LOG.info("producer inserted message : " + messageBody);

                return null;
            };
            pushers.add(worker);
        }

        // execute all pushers in parallel
        executor.invokeAll(pushers);


        // --------------------------------------------------- //
        // 2- Run multiple concurrent consumers simultaneously //
        // --------------------------------------------------- //

        // pull and remove concurrently message from queue
        for (int i = 0; i < 59; i++) {
            Callable<String> workerPuller = () -> {

                MessageQueue messageQueue = queueService.pull(queueUrl);

                LOG.info("messageQueue pulled by consumer : " + messageQueue);

                return messageQueue.getReceiptHandle();

            };


            // execute consumer threads and retrieve the returned receipt handle
            String receiptHandle = executor.submit(workerPuller).get();

            Runnable workerRemover = () -> {

                queueService.delete(queueUrl, receiptHandle);

                LOG.info("consumer deleted message with receipt handle : " + receiptHandle);

            };

            // execute remover threads
            if(i % 2 == 0) {
                executor.execute(workerRemover);
            }


        }

        // ----------------------------------- //
        // 3- Shutting down executor instances //
        // ----------------------------------- //

        executor.shutdown();
        // Wait until all threads are finish
        executor.awaitTermination(10000L, TimeUnit.SECONDS);


        // ---------------------------------------------------------------------------------------------------- //
        // 4- Pulling messages randomly and at regular time interval to verify the visibility monitor execution //
        // ---------------------------------------------------------------------------------------------------- //


        // verify the execution of Visibility monitor that should progressively
        // reactivate the invisible messages still in the queue

        Thread.sleep(2000L);

        MessageQueue messageQueue = queueService.pull(queueUrl);

        LOG.info("consumer pulled message : " + messageQueue);

        Thread.sleep(2000L);

        messageQueue = queueService.pull(queueUrl);

        LOG.info("consumer pulled message : " + messageQueue);

        Thread.sleep(2000L);

        messageQueue = queueService.pull(queueUrl);

        LOG.info("consumer pulled message : " + messageQueue);

        // pause 30 secs to verify how many messages the visibility monitor thread has re-enable
        Thread.sleep(30000L);
    }

}
