package com.canva.queue.service.file;

import com.canva.queue.common.exception.QueueServiceException;
import com.canva.queue.message.MessageQueue;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.hamcrest.core.IsNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.*;

public class FileQueueServiceTest {

    @Test
    public void shouldDetectWhenVisibilityTimeoutExpired() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        PrintWriter mockPrintWriter = mock(PrintWriter.class);
        BufferedReader mockReader = mock(BufferedReader.class);
        File mockFile = mock(File.class);
        // set visibility timestamp far in the future to make the message invisible
        long futureTimestamp = DateTime.now().plusHours(1).getMillis();
        String invisibleMessage = "0:" + futureTimestamp + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";

        doReturn(mockPrintWriter).when(fileQueueService).getPrintWriter(any());
        doReturn(mockReader).when(fileQueueService).getBufferedReader(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());
        doNothing().when(fileQueueService).replaceWithNewFile(any(), any());
        when(mockReader.lines()).thenReturn(Stream.of(invisibleMessage), Stream.of(invisibleMessage));

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        // first call should not pull the message as the visibility timestamp is set to 1 hour in the future
        MessageQueue messageQueueStillInvisible = fileQueueService.pull(queueUrl);
        // second call should pull the message after having fixed the current system millis to 2 hours in the future
        DateTimeUtils.setCurrentMillisFixed(DateTime.now().plusHours(2).getMillis());
        MessageQueue messageQueueVisible = fileQueueService.pull(queueUrl);


        // then
        then(fileQueueService).should(times(2)).getMessagesFile("MyQueue");
        then(fileQueueService).should(times(2)).getLockFile("MyQueue");
        then(fileQueueService).should(times(2)).lock(any());
        then(fileQueueService).should(times(2)).unlock(any());
        then(fileQueueService).should(times(1)).writeNewVisibilityToFile(any(), any(), any());
        then(fileQueueService).should(times(1)).replaceWithNewFile(any(), any());

        assertThat(messageQueueStillInvisible, is(IsNull.nullValue()));
        assertThat(messageQueueVisible, is(IsNull.notNullValue()));
    }

    @Test
    public void shouldIdentifyVisibleMessage() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        String visibleMessage = "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        long pastTimestamp = DateTime.now().minusSeconds(1).getMillis();
        String visibleAgainMessage = "0:" + pastTimestamp + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        long futureTimestamp = DateTime.now().plusMinutes(5).getMillis();
        String invisibleMessage = "0:" + futureTimestamp + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";

        // when
        boolean vm = fileQueueService.isVisibleLine(visibleMessage);
        boolean vam = fileQueueService.isVisibleLine(visibleAgainMessage);
        boolean ivm = fileQueueService.isVisibleLine(invisibleMessage);

        // then
        assertTrue(vm);
        assertTrue(vam);
        assertFalse(ivm);
    }

    @Test
    public void shouldValidateMessages() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        String validMessage = "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        String invalidMessage = "0:invalid message:";

        // when
        String message = fileQueueService.validateMessage(validMessage);

        try {
            fileQueueService.validateMessage(invalidMessage);
            fail("should not go further and throw an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "message line invalid '0:invalid message:'");
        }

        // then
        assertEquals(validMessage, message);
    }

    @Test
    public void shouldUpdateMessageVisibility() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        String validMessage = "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";

        // when
        String resetMessage = fileQueueService.updateMessageVisibility(validMessage, 10);

        // then
        List<String> recordFields = Lists.newArrayList(Splitter.on(":").split(resetMessage));

        assertTrue(Longs.tryParse(recordFields.get(1)) >= (DateTime.now().getMillis()));
    }

    @Test
    public void shouldRetrieveMessageId() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        String messageId = "1e894d74-a39a-4707-b0a7-f3b444960139";
        String message = "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:" + messageId + ":message body test";

        // when
        Optional<String> messageIdFound = fileQueueService.retrieveMessageId(message);

        // then

        assertEquals(messageId, messageIdFound.get());
    }

    @Test
    public void shouldWriteNewVisibilityToFile() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        String messageWithFormerVisibility = "0:0:a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        long futureTimestamp = DateTime.now().plusSeconds(2).getMillis();
        String messageWithNewVisibility = "0:" + futureTimestamp + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";
        List<String> lines = Lists.newArrayList(messageWithFormerVisibility);
        Supplier<Stream<String>> streamSupplier =
                () -> Stream.of(Iterators.toArray(lines.iterator(), String.class));
        PrintWriter mockPrintWriter = mock(PrintWriter.class);

        // when
        fileQueueService.writeNewVisibilityToFile(streamSupplier, mockPrintWriter, messageWithNewVisibility);

        // then
        verify(mockPrintWriter).println(messageWithNewVisibility);
    }

    @Test
    public void shouldNotWriteNewVisibilityToFileAndThrowExceptionWhenCannotRetrieveMessageId() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        String messageWithInvalidMessageId = "0:0";
        List<String> lines = Lists.newArrayList(messageWithInvalidMessageId);
        Supplier<Stream<String>> streamSupplier =
                () -> Stream.of(Iterators.toArray(lines.iterator(), String.class));
        PrintWriter mockPrintWriter = mock(PrintWriter.class);

        // when
        try {
            fileQueueService.writeNewVisibilityToFile(streamSupplier, mockPrintWriter, messageWithInvalidMessageId);
            fail("should not write message to file");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "message line invalid '0:0'");
        }

        // then
        then(mockPrintWriter).should(times(0)).println();
    }

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
            assertEquals("queueName must not be empty", e.getMessage());
        }

        // then
        then(fileQueueService).should(times(0)).lock(any());
        then(fileQueueService).should(times(0)).getPrintWriter(any());
    }

    @Test
    public void shouldNotPushMessageAndThrowQueueServiceExceptionWhenIOExceptionOccurred() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        File mockFile = mock(File.class);

        doThrow(IOException.class).when(fileQueueService).getPrintWriter(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        Integer delays = null;
        String messageBody = "message body test";

        try {
            fileQueueService.push(queueUrl, delays, messageBody);
            fail("should not push message if queueUrl is invalid");
        } catch (QueueServiceException e) {
            assertEquals(e.getMessage(), "An error occurred while pushing messages [message body test] to file 'null'");
        }

        // then
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).getPrintWriter(any());
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
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).writeNewVisibilityToFile(any(), any(), any());
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
        // invisible message with visibility timeout set to 1 hour in the future
        String line = "0:" + DateTime.now().plusHours(1).getMillis() + ":a8bd335e-e202-4bf2-aff7-b94a5a12571f:1e894d74-a39a-4707-b0a7-f3b444960139:message body test";

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
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(0)).writeNewVisibilityToFile(any(), any(), any());
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
        then(fileQueueService).should(times(0)).writeNewVisibilityToFile(any(), any(), any());
        then(fileQueueService).should(times(0)).replaceWithNewFile(any(), any());
    }

    @Test
    public void shouldNotPullMessageAndThrowQueueServiceExceptionWhenIOExceptionOccurred() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        File mockFile = mock(File.class);
        BufferedReader mockReader = mock(BufferedReader.class);

        doThrow(IOException.class).when(fileQueueService).getPrintWriter(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doReturn(mockReader).when(fileQueueService).getBufferedReader(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        try {
            fileQueueService.pull(queueUrl);
            fail("should not push message if queueUrl is invalid");
        } catch (QueueServiceException e) {
            assertEquals(e.getMessage(), "An exception occurred while pulling from queue 'MyQueue'");
        }

        // then
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).getPrintWriter(any());
        then(fileQueueService).should(times(0)).writeNewVisibilityToFile(any(), any(), any());
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
    public void shouldNotDeleteMessageAndThrowExceptionWhenReceiptHandleIsNull() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/";
        String receiptHandle = null;

        try {
            fileQueueService.delete(queueUrl, receiptHandle);
            fail("should not push message if queueUrl is invalid");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "receipt handle must not be null");
        }

        // then
        then(fileQueueService).should(times(0)).lock(any());
        then(fileQueueService).should(times(0)).unlock(any());
        then(fileQueueService).should(times(0)).replaceWithNewFile(any(), any());
    }

    @Test
    public void shouldNotDeleteMessageAndThrowQueueServiceExceptionWhenIOExceptionOccurred() throws Exception {

        // given
        FileQueueService fileQueueService = spy(FileQueueService.class);
        File mockFile = mock(File.class);

        doThrow(IOException.class).when(fileQueueService).getLinesFromFileMessages(any());
        doReturn(mockFile).when(fileQueueService).getNewMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getMessagesFile(any());
        doReturn(mockFile).when(fileQueueService).getLockFile(any());
        doNothing().when(fileQueueService).lock(any());
        doNothing().when(fileQueueService).unlock(any());

        // when
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
        String receiptHandle = "a8bd335e-e202-4bf2-aff7-b94a5a12571f";

        try {
            fileQueueService.delete(queueUrl, receiptHandle);
            fail("should not push message if queueUrl is invalid");
        } catch (QueueServiceException e) {
            assertEquals(e.getMessage(), "An exception occurred while deleting receiptHandle 'a8bd335e-e202-4bf2-aff7-b94a5a12571f'");
        }

        // then
        then(fileQueueService).should(times(1)).lock(any());
        then(fileQueueService).should(times(1)).unlock(any());
        then(fileQueueService).should(times(1)).getLinesFromFileMessages(any());
        then(fileQueueService).should(times(0)).writeLinesToNewFile(any(), any());
        then(fileQueueService).should(times(0)).replaceWithNewFile(any(), any());
    }

}