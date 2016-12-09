package com.canva.queue.service.memory;

import com.canva.queue.message.MessageQueue;
import org.hamcrest.core.IsNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.then;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class InMemoryQueueServiceTest {

    @Test
    public void shouldDetectWhenVisibilityTimeoutExpired() throws Exception {

        // given
        InMemoryQueueService.VisibilityMessageMonitor visibilityMessageMonitor = spy(new InMemoryQueueService().new VisibilityMessageMonitor());

        // mock invisible message
        MessageQueue inVisibleMessageQueue = mock(MessageQueue.class);

        // set visibility timestamp far in the future to make the message invisible
        long futureTimestamp = DateTime.now().plusHours(1).getMillis();
        when(inVisibleMessageQueue.getVisibility()).thenReturn(futureTimestamp);

        // add invisible message to map queue
        ConcurrentLinkedQueue queueWithInvisibleMessage = new ConcurrentLinkedQueue();
        queueWithInvisibleMessage.offer(inVisibleMessageQueue);
        ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> mapQueueWithInvisibleMessage = new ConcurrentHashMap<>();
        mapQueueWithInvisibleMessage.put("myQueue", queueWithInvisibleMessage);

        // return map queue with invisible message when getMessageQueue is invoked
        doReturn(mapQueueWithInvisibleMessage).when(visibilityMessageMonitor).getMessageQueue();


        // when

        // first call should not pull the message as the visibility timestamp is set to 1 hour in the future
        visibilityMessageMonitor.checkMessageVisibility();
        // second call should pull the message after having fixed the current system millis to 2 hours in the future
        DateTimeUtils.setCurrentMillisFixed(DateTime.now().plusHours(2).getMillis());
        visibilityMessageMonitor.checkMessageVisibility();


        // then

        // the invisible message should become visible only once during the second call
        // to checkMessageVisibility() whereby its setVisibility() method is invoked
        // to reset its value
        then(inVisibleMessageQueue).should(times(4)).getVisibility();
        then(inVisibleMessageQueue).should(times(1)).setVisibility(0L);
    }

    @Test
    public void shouldPushMessageInMemoryQueue() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);
        ConcurrentLinkedQueue mockQueue = mock(ConcurrentLinkedQueue.class);

        doReturn(mockQueue).when(inMemoryQueueService).getMessagesFromQueue(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        Integer delays = 20;
        String messageBody = "message body test";

        inMemoryQueueService.push(queueUrl, delays, messageBody);


        // then
        verify(inMemoryQueueService).getMessagesFromQueue("MyQueue");
        then(inMemoryQueueService).should(times(0)).putMessagesIntoQueue(any(), any());
        then(inMemoryQueueService).should(times(1)).fromUrl(any());

        ArgumentCaptor<MessageQueue> argumentCaptor = ArgumentCaptor.forClass(MessageQueue.class);
        verify(mockQueue).offer(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue(), is(IsNull.notNullValue()));
        assertThat(argumentCaptor.getValue().getMessageBody(), is("message body test"));
        assertThat(argumentCaptor.getValue().getRequeueCount(), is(0));

    }

    @Test
    public void shouldNotPushMessageAndThrowExceptionWhenQueueUrlIsInvalid() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/";
        Integer delays = 20;
        String messageBody = "message body test";

        try {
            inMemoryQueueService.push(queueUrl, delays, messageBody);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueName must not be empty");
        }

        // then
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(0)).getMessagesFromQueue(any());
    }

    @Test
    public void shouldPullVisibleMessageFromQueue() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);
        ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();
        String messageBody = "message body test";
        MessageQueue visibleMessageQueue = MessageQueue.create(0L, messageBody);
        queue.offer(visibleMessageQueue);

        doReturn(queue).when(inMemoryQueueService).getMessagesFromQueue(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        MessageQueue messageQueue = inMemoryQueueService.pull(queueUrl);


        // then
        verify(inMemoryQueueService).getMessagesFromQueue("MyQueue");
        then(inMemoryQueueService).should(times(0)).putMessagesIntoQueue(any(), any());
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(1)).getMessagesFromQueue(any());
        assertThat(visibleMessageQueue.getVisibility(), is(not(0L)));
        assertThat(messageQueue.getReceiptHandle(), is(IsNull.notNullValue()));
        assertEquals(messageQueue.getMessageBody(), messageBody);

    }

    @Test
    public void shouldNotPullAndReturnNullWhenNoVisibleMessagePresentInQueue() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);
        ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();
        String messageBody = "message body test";
        MessageQueue visibleMessageQueue = MessageQueue.create(245637L, messageBody);
        queue.offer(visibleMessageQueue);

        doReturn(queue).when(inMemoryQueueService).getMessagesFromQueue(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        MessageQueue messageQueue = inMemoryQueueService.pull(queueUrl);


        // then
        verify(inMemoryQueueService).getMessagesFromQueue("MyQueue");
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(1)).getMessagesFromQueue(any());
        assertThat(messageQueue, is(IsNull.nullValue()));

    }

    @Test
    public void shouldNotPullMessageAndThrowExceptionWhenQueueUrlIsNull() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);

        // when
        String queueUrl = null;

        try {
            inMemoryQueueService.pull(queueUrl);
            fail("should not pull message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueUrl must not be empty");
        }

        // then
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(0)).getMessagesFromQueue(any());

    }

    @Test
    public void shouldNotPullMessageAndThrowExceptionWhenQueueUrlIsInvalid() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/";

        try {
            inMemoryQueueService.pull(queueUrl);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueName must not be empty");
        }

        // then
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(0)).getMessagesFromQueue(any());
    }

    @Test
    public void shouldDeleteMessageGivenReceiptHandleArgument() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);
        ConcurrentLinkedQueue<MessageQueue> queue = new ConcurrentLinkedQueue<>();
        String messageBody = "message body test";
        MessageQueue messageQueueWithSameReceiptHandle = MessageQueue.createFromLine("0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960138:message body test");
        MessageQueue messageQueueWithDifferentReceiptHandle = MessageQueue.createFromLine("0:0:a8bd335e-a39a-4bf2-aff7-b94a5a12571d:1e894d74-a39a-4707-b0a7-f3b444960139:message body test");
        queue.offer(messageQueueWithSameReceiptHandle);
        queue.offer(messageQueueWithDifferentReceiptHandle);

        doReturn(queue).when(inMemoryQueueService).getMessagesFromQueue(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        String receiptHandle = "a8bd335e-e202-4bf2-aff7-b94a5a12571f";

        inMemoryQueueService.delete(queueUrl, receiptHandle);


        // then
        verify(inMemoryQueueService).getMessagesFromQueue("MyQueue");
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(1)).getMessagesFromQueue(any());

        assertThat(queue.size(), is(1));
        assertThat(queue.peek().getReceiptHandle(), is(not(receiptHandle)));
        assertThat(queue.peek().getMessageBody(), is(messageBody));
    }

    @Test
    public void shouldNotDeleteMessageAndThrowExceptionWhenQueueUrlIsNull() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);

        // when
        String queueUrl = null;
        String receiptHandle = "a8bd335e-e202-4bf2-aff7-b94a5a12571f";

        try {
            inMemoryQueueService.delete(queueUrl, receiptHandle);
            fail("should not pull message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueUrl must not be empty");
        }

        // then
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(0)).getMessagesFromQueue(any());

    }

    @Test
    public void shouldNotDeleteMessageAndThrowExceptionWhenQueueUrlIsInvalid() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/";
        String receiptHandle = "a8bd335e-e202-4bf2-aff7-b94a5a12571f";

        try {
            inMemoryQueueService.delete(queueUrl, receiptHandle);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueName must not be empty");
        }

        // then
        then(inMemoryQueueService).should(times(1)).fromUrl(any());
        then(inMemoryQueueService).should(times(0)).getMessagesFromQueue(any());
    }

    @Test
    public void shouldNotDeleteMessageAndThrowExceptionWhenReceiptHandleIsNull() throws Exception {

        // given
        InMemoryQueueService inMemoryQueueService = spy(InMemoryQueueService.class);

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/";
        String receiptHandle = null;

        try {
            inMemoryQueueService.delete(queueUrl, receiptHandle);
            fail("should not push message if queueUrl is invalid");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "receipt handle must not be null");
        }

        // then
        then(inMemoryQueueService).should(times(0)).fromUrl(any());
        then(inMemoryQueueService).should(times(0)).getMessagesFromQueue(any());
    }


}