package com.canva.queue.service.file;

import com.canva.queue.message.MessageQueue;
import com.canva.queue.service.memory.InMemoryQueueService;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.*;

public class FileQueueServiceTest {

    @Test
    public void shouldPushMessageToQueueWhenDelayQueueUrlAndMessageBodyAreValid() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        PrintWriter mockPrintWriter = mock(PrintWriter.class);
        File mockFile = mock(File.class);

        doReturn(mockPrintWriter).when(fileQueueService).getPrintWriter(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());


        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        Integer delays = 20;
        String messageBody = "message body test";

        fileQueueService.push(queueUrl, delays, messageBody);


        // then
        verify(fileQueueService).getMessagesFile("MyQueue");
        verify(fileQueueService).getLockFile("MyQueue");
        then(fileQueueService).should(times(1)).addToVisibleQueueList("MyQueue");
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());

        ArgumentCaptor<String> captorNoOfRequestedWheels = ArgumentCaptor.forClass(String.class);
        verify(mockPrintWriter).println(captorNoOfRequestedWheels.capture());
        assertThat(captorNoOfRequestedWheels.getValue().split(":").length, is(5));
        assertThat(Lists.newArrayList(captorNoOfRequestedWheels.getValue().split(":")).get(1), is(not("0")));
        assertThat(Lists.newArrayList(captorNoOfRequestedWheels.getValue().split(":")).get(4), is("message body test"));
    }

    @Test
    public void shouldNotPushMessageAndThrowExceptionWhenQueueUrlIsNull() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        PrintWriter mockPrintWriter = mock(PrintWriter.class);

        // when
        String queueUrl = null;
        Integer delays = 20;
        String messageBody = "message body test";

        try {
            fileQueueService.push(queueUrl, delays, messageBody);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueUrl must not be empty");
        }

        // then
        then(fileQueueService).should(times(0)).lock(any());
        then(fileQueueService).should(times(0)).getPrintWriter(any());
        then(fileQueueService).should(times(0)).addToVisibleQueueList(any());
    }

    @Test
    public void shouldNotPushMessageAndThrowExceptionWhenQueueUrlIsInvalid() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/";
        Integer delays = 20;
        String messageBody = "message body test";

        try {
            fileQueueService.push(queueUrl, delays, messageBody);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueName must not be null");
        }

        // then
        then(fileQueueService).should(times(0)).lock(any());
        then(fileQueueService).should(times(0)).getPrintWriter(any());
        then(fileQueueService).should(times(0)).addToVisibleQueueList(any());
    }

    @Test
    public void shouldPushAndSetVisibilityToZeroWhenDelayIsNull() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        PrintWriter mockPrintWriter = mock(PrintWriter.class);
        File mockFile = mock(File.class);

        doReturn(mockPrintWriter).when(fileQueueService).getPrintWriter(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());


        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        Integer delays = null;
        String messageBody = "message body test";

        fileQueueService.push(queueUrl, delays, messageBody);


        // then
        verify(fileQueueService).getMessagesFile("MyQueue");
        verify(fileQueueService).getLockFile("MyQueue");
        then(fileQueueService).should(times(0)).addToVisibleQueueList("MyQueue");
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockPrintWriter).println(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().split(":").length, is(5));
        assertThat(Lists.newArrayList(argumentCaptor.getValue().split(":")).get(1), is("0"));
        assertThat(Lists.newArrayList(argumentCaptor.getValue().split(":")).get(4), is("message body test"));
    }

    @Test
    public void shouldPullVisibleMessages() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        PrintWriter mockPrintWriter = mock(PrintWriter.class);
        BufferedReader mockReader = mock(BufferedReader.class);
        File mockFile = mock(File.class);
        String line = "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";

        doReturn(mockPrintWriter).when(fileQueueService).getPrintWriter(any());
        doReturn(mockReader).when(fileQueueService).getBufferedReader(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());
        doNothing().when(fileQueueService).replaceWithNewFile(any(), any());
        when(mockReader.lines()).thenReturn(Stream.of(line));

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        MessageQueue messageQueue = fileQueueService.pull(queueUrl);


        // then
        verify(fileQueueService).getMessagesFile("MyQueue");
        verify(fileQueueService).getLockFile("MyQueue");
        then(fileQueueService).should(times(1)).addToVisibleQueueList("MyQueue");
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).changeVisibilityAndWriteToFile(any(), any(), any());
        then(fileQueueService).should(times(1)).replaceWithNewFile(any(), any());

        // message body
        assertEquals(messageQueue.getMessageBody(), "message body test");
        // receiptHandle
        assertEquals(messageQueue.getReceiptHandle(), "a8bd335e-e202-4bf2-aff7-b94a5a12571f");

    }


    @Test
    public void shouldNotPullAnyMessageAndReturnNullWhenThereIsNoVisibleMessages() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        PrintWriter mockPrintWriter = mock(PrintWriter.class);
        BufferedReader mockReader = mock(BufferedReader.class);
        File mockFile = mock(File.class);
        String line = "0:1480950771794:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";

        doReturn(mockPrintWriter).when(fileQueueService).getPrintWriter(any());
        doReturn(mockReader).when(fileQueueService).getBufferedReader(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());
        when(mockReader.lines()).thenReturn(Stream.of(line));

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        MessageQueue messageQueue = fileQueueService.pull(queueUrl);


        // then
        verify(fileQueueService).getMessagesFile("MyQueue");
        verify(fileQueueService).getLockFile("MyQueue");
        then(fileQueueService).should(times(1)).addToVisibleQueueList("MyQueue");
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(0)).changeVisibilityAndWriteToFile(any(), any(), any());
        then(fileQueueService).should(times(0)).replaceWithNewFile(any(), any());

        assertTrue(messageQueue == null);

    }


    @Test
    public void shouldNotPullMessageAndThrowExceptionWhenQueueUrlIsNull() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);

        // when
        String queueUrl = null;

        try {
            fileQueueService.pull(queueUrl);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueUrl must not be empty");
        }

        // then
        then(fileQueueService).should(times(0)).lock(any());
        then(fileQueueService).should(times(0)).unlock(any());
        then(fileQueueService).should(times(0)).getPrintWriter(any());
        then(fileQueueService).should(times(0)).addToVisibleQueueList(any());
        then(fileQueueService).should(times(0)).changeVisibilityAndWriteToFile(any(), any(), any());
        then(fileQueueService).should(times(0)).replaceWithNewFile(any(), any());
    }

    @Test
    public void shouldDeleteMessageFromFileGivenReceiptHandleArgument() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        File mockFile = mock(File.class);
        List<String> lines = Lists.newArrayList(
                "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test",
                "0:0:a8bd335e-a39a-4bf2-aff7-b94a5a12571d:1e894d74-a39a-4707-b0a7-f3b444960139:message body test");

        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doReturn(lines).when(fileQueueService).getLinesFromFileMessages(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());
        doNothing().when(fileQueueService).replaceWithNewFile(any(), any());
        doNothing().when(fileQueueService).writeLinesToNewFile(any(), any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        String receiptHandle = "a8bd335e-e202-4bf2-aff7-b94a5a12571f";

        fileQueueService.delete(queueUrl, receiptHandle);


        // then
        verify(fileQueueService).getMessagesFile("MyQueue");
        verify(fileQueueService).getLockFile("MyQueue");
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).writeLinesToNewFile(any(), any());
        then(fileQueueService).should(times(1)).replaceWithNewFile(any(), any());

        ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(fileQueueService).writeLinesToNewFile(any(), argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().size(), is(1));
        assertThat(argumentCaptor.getValue().get(0), is("0:0:a8bd335e-a39a-4bf2-aff7-b94a5a12571d:1e894d74-a39a-4707-b0a7-f3b444960139:message body test"));

    }

    @Test
    public void shouldNotDeleteInputMessageAndThrowExceptionWhenQueueUrlIsNull() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);

        // when
        String queueUrl = null;
        String receiptHandle = "a8bd335e-e202-4bf2-aff7-b94a5a12571f";

        try {
            fileQueueService.delete(queueUrl, receiptHandle);
            fail("should not push message if queueUrl is invalid");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "queueUrl must not be empty");
        }

        // then
        then(fileQueueService).should(times(0)).lock(any());
        then(fileQueueService).should(times(0)).unlock(any());
        then(fileQueueService).should(times(0)).replaceWithNewFile(any(), any());
    }

    @Test
    public void shouldResetMessageVisibility() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        FileQueueService.VisibilityMessageMonitor visibilityMessageMonitor = spy(fileQueueService.new VisibilityMessageMonitor());
        File mockFile = mock(File.class);

        // list of visible and invisible messages
        long pastTimestamp = DateTime.now().minusSeconds(1).getMillis();
        String invisibleMessage = "0:" + pastTimestamp + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        String visibleMessage = "0:0:a8bd335e-a39a-4bf2-aff7-b94a5a12571d:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        String expectedMessageAfterVisibilityReset = "0:" + 0 + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        List<String> linesWithMessage = Lists.newArrayList(visibleMessage, invisibleMessage);

        // define the list of queue name
        CopyOnWriteArrayList queueVisibleList = Lists.newCopyOnWriteArrayList();
        queueVisibleList.add("MyQueue");

        // mock of lines read from message file
        List<String> mockLines = mock(List.class);

        // mock returned lines with visible and invisible messages
        when(mockLines.stream()).thenReturn(linesWithMessage.stream());

        doReturn(mockLines).when(visibilityMessageMonitor).readAllLines(any());
        doReturn(queueVisibleList).when(visibilityMessageMonitor).getQueueVisibleList();
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());
        doNothing().when(fileQueueService).replaceWithNewFile(any(), any());
        doNothing().when(fileQueueService).writeLinesToNewFile(any(), any());

        doCallRealMethod().when(visibilityMessageMonitor).checkMessageVisibility();


        // when
        visibilityMessageMonitor.checkMessageVisibility();


        // then
        verify(fileQueueService).getMessagesFile("MyQueue");
        verify(fileQueueService).getLockFile("MyQueue");
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).writeLinesToNewFile(any(), any());
        then(fileQueueService).should(times(1)).replaceWithNewFile(any(), any());

        ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(fileQueueService, times(1)).writeLinesToNewFile(any(), argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().size(), is(2));
        // visible message line remains unchanged
        assertThat(argumentCaptor.getValue().get(0), is(visibleMessage));
        // invisible message is now visible after being updated by monitor
        assertThat(argumentCaptor.getValue().get(1), is(expectedMessageAfterVisibilityReset));

    }

}