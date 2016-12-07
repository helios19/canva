package com.canva.queue.message;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Class representing a message queue and holding the message details, namely its requeue count,
 * receipt handle, message id, visibility and message body.
 *
 * <p>This class is intended to be used internally by the {@link com.canva.queue.service.QueueService}
 * implementation classes as it provides some mutable methods essentially to compare and update the visibility value
 * in an atomic manner. Note that the {@link com.canva.queue.service.QueueService} implementation classes will instead
 * return an immutable version of this class.
 *
 * @see com.canva.queue.service.QueueService
 * @see ImmutableMessageQueue
 */
public class MessageQueue implements Serializable, Cloneable {

    private Integer requeueCount;

    private String receiptHandle;

    private String messageId;

    private AtomicLong visibility = new AtomicLong(0L);

    private String messageBody;

    protected MessageQueue() {
    }

    public MessageQueue(String messageBody) {
        this.requeueCount = 0;
        this.receiptHandle = UUID.randomUUID().toString();
        this.messageId = UUID.randomUUID().toString();
        this.messageBody = requireNonNull(messageBody);
    }

    public MessageQueue(Integer requeueCount, String receiptHandle, String messageId, String messageBody) {
        this.requeueCount = requireNonNull(requeueCount);
        this.receiptHandle = requireNonNull(receiptHandle);
        this.messageId = requireNonNull(messageId);
        this.messageBody = requireNonNull(messageBody);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageQueue messageQueue = (MessageQueue) o;
        return Objects.equal(messageId, messageQueue.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requeueCount", requeueCount)
                .add("visibility", visibility.get())
                .add("receiptHandle", receiptHandle)
                .add("messageId", messageId)
                .add("messageBody", messageBody)
                .toString();
    }

    /**
     * Returns a comma-separated string concatenation of the instance property values.
     *
     * @return String value
     */
    public String writeToString() {
        return Joiner.on(":").skipNulls().join(requeueCount, visibility.get(), receiptHandle, messageId, messageBody);
    }

    public String getMessageBody() {
        return messageBody;
    }

    public Long getVisibility() {
        return visibility.get();
    }

    public Integer getRequeueCount() {
        return requeueCount;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public String getMessageId() {
        return messageId;
    }

    /**
     * Compares and sets atomically the visibility value by invoking {@link AtomicLong#compareAndSet(long, long)} method.
     *
     * @param expect  Expected value
     * @param update  Update value
     * @return true if operation succeed, false otherwise
     */
    public boolean compareAndSetVisibility(long expect, long update) {
        return visibility.compareAndSet(expect, update);
    }

    /**
     * Resets the visibility value.
     *
     * @param visibility New visibility to set
     */
    public void setVisibility(long visibility) {
        this.visibility.set(visibility);
    }

    /**
     * Creates a {@link MessageQueue} instance given {@code visibileFrom} and {@code messageBody} arguments.
     *
     * @param visibileFrom  Visibility value
     * @param messageBody  Message body value
     * @return MessageQueue instance
     */
    public static MessageQueue create(long visibileFrom, String messageBody) {
        MessageQueue messageQueue = new MessageQueue(messageBody);
        messageQueue.visibility.set(visibileFrom);
        return messageQueue;
    }

    /**
     * Creates a {@link MessageQueue} instance from the given {@code messageLine} argument.
     *
     * @param messageLine String representation of a MessageQueue
     * @return MessageQueue instance
     */
    public static MessageQueue createFromLine(String messageLine) {
        List<String> recordFields = Lists.newArrayList(Splitter.on(":").split(messageLine));

        String requeueCount = recordFields.get(0);
        String visibility = recordFields.get(1);
        String receiptHandle = recordFields.get(2);
        String messageId = recordFields.get(3);
        String messageBody = recordFields.get(4);

        MessageQueue messageQueue = new MessageQueue(Ints.tryParse(requeueCount), receiptHandle, messageId, messageBody);
        messageQueue.visibility.set(Longs.tryParse(visibility));

        return messageQueue;
    }
}
