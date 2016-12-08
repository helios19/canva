package com.canva.queue.message;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Class representing an immutable version of the {@link MessageQueue} class. The main intent of this class is to be used
 * by callers of {@link com.canva.queue.service.QueueService} implementation classes that will return it in order to keep
 * the internal state of the queue messages safe.
 *
 * @see MessageQueue
 */
public final class ImmutableMessageQueue extends MessageQueue implements Serializable, Cloneable {

    private Integer requeueCount;

    private String receiptHandle;

    private String messageId;

    private Long visibility;

    private String messageBody;

    public ImmutableMessageQueue(Integer requeueCount, String receiptHandle,
                                 String messageId, Long visibilityDelay, String messageBody) {
        this.requeueCount = requireNonNull(requeueCount);
        this.receiptHandle = requireNonNull(receiptHandle);
        this.messageId = requireNonNull(messageId);
        this.visibility = requireNonNull(visibilityDelay);
        this.messageBody = requireNonNull(messageBody);
    }

    public static ImmutableMessageQueue of(MessageQueue messageQueue) {
        requireNonNull(messageQueue);
        return new ImmutableMessageQueue(
                messageQueue.getRequeueCount(),
                messageQueue.getReceiptHandle(),
                messageQueue.getMessageId(),
                messageQueue.getVisibility(),
                messageQueue.getMessageBody());
    }

    public static ImmutableMessageQueue of(Integer requeueCount, String receiptHandle,
                                           String messageId, Long visibilityDelay, String messageBody) {
        return new ImmutableMessageQueue(
                requeueCount,
                receiptHandle,
                messageId,
                visibilityDelay,
                messageBody);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ImmutableMessageQueue that = (ImmutableMessageQueue) o;
        return Objects.equal(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), messageId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requeueCount", requeueCount)
                .add("receiptHandle", receiptHandle)
                .add("messageId", messageId)
                .add("visibility", visibility)
                .add("messageBody", messageBody)
                .toString();
    }

    /**
     * Operation not supported by this immutable type.
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean compareAndSetVisibility(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported by this immutable type.
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public void setVisibility(long visibility) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported by this immutable type.
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public String writeToString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMessageBody() {
        return messageBody;
    }

    @Override
    public Long getVisibility() {
        return visibility;
    }

    @Override
    public Integer getRequeueCount() {
        return requeueCount;
    }

    @Override
    public String getReceiptHandle() {
        return receiptHandle;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

}
