package com.canva.queue.service.sqs;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.canva.queue.message.ImmutableMessageQueue;
import com.canva.queue.message.MessageQueue;
import com.canva.queue.service.QueueService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;

/**
 * SQS adapter class providing standard operations to push, pull and remove messages from a AWS SQS Queue.
 *
 * <p>This class uses a {@link AmazonSQSClient} client to access AWS SQS Queue.
 *
 * @see com.canva.queue.service.QueueService
 * @see AmazonSQSClient
 */
public class SqsQueueService implements QueueService {

    private static final Log LOG = LogFactory.getLog(SqsQueueService.class);

    /**
     * SQS client
     */
    private AmazonSQSClient sqsClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {
        sqsClient.sendMessage(
                new SendMessageRequest(queueUrl, messageBody).withDelaySeconds(delaySeconds)
        );
    }

    /**
     * {@inheritDoc}
     *
     * Note that this method pulls only the first top message among the list of messages returned by SQS client.
     */
    @Override
    public MessageQueue pull(String queueUrl) {
        Optional<Message> message = sqsClient.receiveMessage(queueUrl).getMessages().stream().findFirst();

        if(!message.isPresent()) {
            LOG.info("no message found from queueUrl '" + queueUrl + "'");
        }

        return ImmutableMessageQueue.of(
                null,
                message.get().getReceiptHandle(),
                message.get().getMessageId(),
                null,
                message.get().getBody());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String queueUrl, String receiptHandle) {
        sqsClient.deleteMessage(queueUrl, receiptHandle);
    }
}
