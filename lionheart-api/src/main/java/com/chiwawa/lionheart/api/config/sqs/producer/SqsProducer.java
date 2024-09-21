package com.chiwawa.lionheart.api.config.sqs.producer;

import static com.chiwawa.lionheart.common.exception.ErrorCode.SQS_EXCEPTION;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.chiwawa.lionheart.common.constant.MessageType;
import com.chiwawa.lionheart.common.dto.sqs.MessageDto;
import com.chiwawa.lionheart.common.exception.model.InternalServerException;
import com.chiwawa.lionheart.common.util.MessageUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SqsProducer {

    @Value("${cloud.aws.sqs.notification.url}")
    private String notificationUrl;

    private static final String messageGroupId = "sqs";
    private final ObjectMapper objectMapper;
    private final AmazonSQS amazonSqs;
    private static final String SQS_QUEUE_REQUEST_LOG_MESSAGE = "====> [SQS Queue Request] : %s ";

    public SqsProducer(ObjectMapper objectMapper, AmazonSQS amazonSqs) {
        this.objectMapper = objectMapper;
        this.amazonSqs = amazonSqs;
    }

    @Retryable(
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000),
            include = {AmazonSQSException.class},
            listeners = {"SqsRetryListener"})
    public void produce(MessageDto dto) {
        try {
            SendMessageRequest sendMessageRequest = new SendMessageRequest(notificationUrl,
                    objectMapper.writeValueAsString(dto))
                    .withMessageGroupId(messageGroupId)
                    .withMessageDeduplicationId(UUID.randomUUID().toString())
                    .withMessageAttributes(createMessageAttributes(dto.getType()));
            amazonSqs.sendMessage(sendMessageRequest);
            log.info(MessageUtils.generate(SQS_QUEUE_REQUEST_LOG_MESSAGE, dto));
        } catch (JsonProcessingException exception) {
            throw new AmazonSQSException("Message sending failed by json processing.");
        }
    }

    private Map<String, MessageAttributeValue> createMessageAttributes(String type) {
        final String dataType = "String";
        return Map.of(MessageType.MESSAGE_TYPE_HEADER, new MessageAttributeValue()
                .withDataType(dataType)
                .withStringValue(type));
    }
}
