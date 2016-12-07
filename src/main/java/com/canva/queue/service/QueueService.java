package com.canva.queue.service;

import com.canva.queue.message.MessageQueue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

public interface QueueService {

    /**
     * Pushes a message at the end of a queue given {@code queueUrl}, {@code delaySeconds} and {@code messageBody} arguments.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @param delaySeconds  Message visibility delay in seconds
     * @param messageBody  Message body to push
     * @throws InterruptedException Thrown in case of issue while locking the resource to access the queue file
     * @throws IOException Thrown in case of issue while accessing the queue message file
     * @throws IllegalArgumentException Thrown in case queueUrl is invalid or queue cannot be extracted
     */
    void push(String queueUrl, Integer delaySeconds, String messageBody);

    /**
     * Pulls a message from the top of the queue given {@code queueUrl} argument.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @return  MessageQueue instance made up with message body and receiptHandle identifier used to delete the message
     * @throws  IllegalArgumentException If queue name cannot be extracted from queueUrl argument
     */
    MessageQueue pull(String queueUrl);

    /**
     * Deletes a message from the queue given {@code queueUrl} and {@code receiptHandle} arguments.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @param receiptHandle  Receipt handle identifier
     */
    void delete(String queueUrl, String receiptHandle);

    /**
     * Extracts queue name from a given {@code queueUrl} argument value.
     *
     * @param queueUrl Queue url
     * @return Queue name
     */
    default String fromUrl(String queueUrl) {
        checkArgument(!Strings.isNullOrEmpty(queueUrl), "queueUrl must not be empty");

        return Iterables.getLast(Splitter.on("/").split(queueUrl), null);
    }




  //
  // Task 1: Define me.
  //
  // This interface should include the following methods.  You should choose appropriate
  // signatures for these methods that prioritise simplicity of implementation for the range of
  // intended implementations (in-memory, file, and SQS).  You may include additional methods if
  // you choose.
  //
  // - push
  //   pushes a message onto a queue.
  // - pull
  //   retrieves a single message from a queue.
  // - delete
  //   deletes a message from the queue that was received by pull().
  //

}
