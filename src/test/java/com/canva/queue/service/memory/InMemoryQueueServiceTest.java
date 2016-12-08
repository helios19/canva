package com.canva.queue.service.memory;

import com.canva.queue.message.MessageQueue;
import org.hamcrest.core.IsNull;
import org.joda.time.DateTime;
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
    public void shouldResetMessageVisibility() throws Exception {

        // given
        InMemoryQueueService.VisibilityMessageMonitor visibilityMessageMonitor = spy(new InMemoryQueueService().new VisibilityMessageMonitor());

        // mock map queue
        ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> mockMapQueue = mock(ConcurrentHashMap.class);

        // visible map queue and message
        MessageQueue visibleMessageQueue = mock(MessageQueue.class);
        when(visibleMessageQueue.getVisibility()).thenReturn(0L);
        ConcurrentLinkedQueue queueWithVisibleMessage = new ConcurrentLinkedQueue();
        queueWithVisibleMessage.offer(visibleMessageQueue);

        ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> messageQueueWithVisibleMessage = new ConcurrentHashMap<>();
        messageQueueWithVisibleMessage.put("myQueue", queueWithVisibleMessage);

        // invisible map queue and message
        long pastTimestamp = DateTime.now().minusSeconds(1).getMillis();
        MessageQueue inVisibleMessageQueue = mock(MessageQueue.class);
        when(inVisibleMessageQueue.getVisibility()).thenReturn(pastTimestamp);
        ConcurrentLinkedQueue queueWithInvisibleMessage = new ConcurrentLinkedQueue();
        queueWithVisibleMessage.offer(inVisibleMessageQueue);

        ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> messageQueueWithInvisibleMessage = new ConcurrentHashMap<>();
        messageQueueWithInvisibleMessage.put("myQueue", queueWithInvisibleMessage);


        // when call getMessageQueue return queue mock
        doReturn(mockMapQueue).when(visibilityMessageMonitor).getMessageQueue();

        // when invoking mock message queue return first a queue with visible messages
        // and then another queue with invisible messages
        when(mockMapQueue.keySet()).thenReturn(
                // first call return a message queue with only visible message
                messageQueueWithVisibleMessage.keySet(),
                // second call return a message queue with invisible message
                messageQueueWithInvisibleMessage.keySet());

        when(mockMapQueue.get(anyString())).thenReturn(
                // first call return a message queue with only visible message
                messageQueueWithVisibleMessage.get("myQueue"),
                // second call return a message queue with invisible message
                messageQueueWithInvisibleMessage.get("myQueue"));


        // when

        // first call -> with visible message
        visibilityMessageMonitor.checkMessageVisibility();
        // second call -> with invisible message where their visibility timestamp exceeds timeout
        visibilityMessageMonitor.checkMessageVisibility();


        // then

        // visible message should not get its visibility reset
        then(inVisibleMessageQueue).should(times(2)).getVisibility();
        then(inVisibleMessageQueue).should(times(1)).setVisibility(0L);
        // invisible message should have its visibility reset to 0L
        then(visibleMessageQueue).should(times(1)).getVisibility();
        then(visibleMessageQueue).should(times(0)).setVisibility(0L);

    }


}