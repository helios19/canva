package com.canva.queue.service.memory;

import com.canva.queue.common.service.AbstractQueueService;
import com.canva.queue.message.ImmutableMessageQueue;
import com.canva.queue.message.MessageQueue;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Queue Service class providing standard operations to push, pull and remove messages into a memory-based FIFO Queue.
 *
 * <p>Messages are stored into a {@link java.util.concurrent.ConcurrentLinkedQueue concurrent FIFO queue} saved into
 * another {@link java.util.concurrent.ConcurrentHashMap concurrent map} keyed by queue name. As a result, this
 * class doesn't used any explicit locking strategy and simply relies on the concurrency mechanism provided by these
 * thread-safe data structure.
 *
 * <p>This class enables a daemon thread used for resetting the messages visibility according to
 * the default {@link com.canva.queue.service.memory.InMemoryQueueService#visibilityTimeoutInSecs}. This thread can be
 * disabled using the constructor
 * {@link com.canva.queue.service.memory.InMemoryQueueService#InMemoryQueueService(java.lang.Integer, boolean)}.
 *
 * @see com.canva.queue.service.QueueService
 * @see com.canva.queue.common.service.AbstractQueueService
 * @see VisibilityMessageMonitor
 */
public class InMemoryQueueService extends AbstractQueueService {

    private static final Log LOG = LogFactory.getLog(InMemoryQueueService.class);

    /**
     * Concurrent map structure used to store messages per queue name into another concurrent FIFO message queue.
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> messageQueue = new ConcurrentHashMap<>();

    protected InMemoryQueueService() {
    }

    public InMemoryQueueService(boolean runVisibilityCollector) {
        startVisibilityCollector(runVisibilityCollector);
    }

    public InMemoryQueueService(Integer visibilityTimeoutInSecs, boolean runVisibilityCollector) {
        this.visibilityTimeoutInSecs = defaultIfNull(visibilityTimeoutInSecs, MIN_VISIBILITY_TIMEOUT_SECS);
        // start message checker
        startVisibilityCollector(runVisibilityCollector);
    }

    private void startVisibilityCollector(boolean runVisibilityCollector) {
        if(runVisibilityCollector) {
            // run visibility message checker
            Thread visibilityMonitor = new Thread(new VisibilityMessageMonitor(), "inMemoryQueueService-visibilityMonitor");
            visibilityMonitor.setDaemon(true);
            visibilityMonitor.start();
        }
    }

    /**
     * Pushes a message at the end of a queue given {@code queueUrl}, {@code delaySeconds} and {@code messageBody} arguments.
     * If {@code delaySeconds} is provided, this method will set the visibility of the message pushed as per below:<br>
     *
     * <pre>{@code
     * visibility = currentTimeMillis + delayInMillis
     * }</pre>
     *
     * @param queueUrl  QueueU url holding the queue name to extract
     * @param delaySeconds  Message visibility delay in seconds
     * @param messageBody  Message body to push
     */
    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {
        String queue = fromUrl(queueUrl);

        ConcurrentLinkedQueue<MessageQueue> messageQueues = getMessagesFromQueue(queue);

        if (messageQueues == null) {
            // use a concurrent list
            messageQueues = putMessagesIntoQueue(queue, new ConcurrentLinkedQueue());
        }

        messageQueues.offer(
                MessageQueue.create(
                        (delaySeconds != null) ? DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds) : 0L,
                        messageBody));
    }

    /**
     * Pulls a message from the top of the queue given {@code queueUrl} argument. The message retrieved must be visible
     * according to its visibility timestamp (i.e equals to 0L). Any message with a different visibility value will be
     * skipped and considered invisible.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @return  MessageQueue instance made up with message body and receiptHandle identifier used to delete the message
     * @throws  IllegalArgumentException If queue name cannot be extracted from queueUrl argument
     */
    @Override
    public MessageQueue pull(String queueUrl) {
        String queue = fromUrl(queueUrl);

        // find first visible record
        for (MessageQueue messageQueue : getMessagesFromQueue(queue)) {
            if (messageQueue.compareAndSetVisibility(
                    0L, DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeoutInSecs))) {
                return ImmutableMessageQueue.of(messageQueue);
            }
        }

        return null;
    }

    /**
     * Deletes a message from the queue given {@code queueUrl} and {@code receiptHandle} arguments.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @param receiptHandle  Receipt handle identifier
     */
    @Override
    public void delete(String queueUrl, String receiptHandle) {
        String queue = fromUrl(queueUrl);

        ConcurrentLinkedQueue<MessageQueue> messageQueues = getMessagesFromQueue(queue);

        if (CollectionUtils.isEmpty(messageQueues)) {
            // remove queue when there are no messages left
            removeEmptyQueue(queue);
        }

        // remove all record with same receiptHandle
        List<MessageQueue> recordsToDelete = messageQueues.stream()
                .filter(record -> StringUtils.equals(record.getReceiptHandle(), receiptHandle))
                .collect(Collectors.toList());

        messageQueues.removeAll(recordsToDelete);
    }

    protected ConcurrentLinkedQueue<MessageQueue> removeEmptyQueue(String queue) {
        return messageQueue.remove(queue);
    }

    protected ConcurrentLinkedQueue<MessageQueue> putMessagesIntoQueue(String queue, ConcurrentLinkedQueue<MessageQueue> messageQueues) {
        messageQueue.put(queue, messageQueues);
        return messageQueues;
    }

    protected ConcurrentLinkedQueue<MessageQueue> getMessagesFromQueue(String queue) {
        return messageQueue.get(queue);
    }

    /**
     * {@inheritDoc}
     */
    protected class VisibilityMessageMonitor extends AbstractVisibilityMonitor {

        protected VisibilityMessageMonitor() {
        }

        /**
         * Check and reset the visibility of messages exceeding the timeout. The update done in this method is still safe
         * as when message.getVisibility() > 0L, the message is considered invisible by the system (consumers/producers threads
         * won't see this message), that is, only the {@link VisibilityMessageMonitor} thread will access it and possibly
         * modify it. Thus, the check and reset can safely happen in a non-atomic manner.
         */
        protected void checkMessageVisibility() {
            requireNonNull(getMessageQueue(), "messageQueue variable must not be null");

            getMessageQueue().keySet().stream().forEach(queue -> getMessageQueue().get(queue)
                    .stream()
                    .filter(messageQueue -> messageQueue.getVisibility() > 0L
                            && DateTime.now().getMillis() > messageQueue.getVisibility())
                    .forEach(messageQueue -> messageQueue.setVisibility(0L)));
        }

        protected ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> getMessageQueue() {
            return messageQueue;
        }
    }
}
