package com.chiwawa.lionheart.api.config.sqs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SqsRetryListener implements RetryListener {
    @Override
    public <T, E extends Throwable> boolean open(RetryContext retryContext, RetryCallback<T, E> retryCallback) {
        log.info("SqsRetryListener Opened");
        return true;
    }

    @Override
    public <T, E extends Throwable> void close(RetryContext retryContext, RetryCallback<T, E> retryCallback,
                                               Throwable throwable) {
        log.info("SqsRetryListener Closed");
    }

    @Override
    public <T, E extends Throwable> void onError(RetryContext retryContext, RetryCallback<T, E> retryCallback,
                                                 Throwable throwable) {
        int retryCount = retryContext.getRetryCount();
        log.info("Sqs Message Produce 과정에서 에러가 발생하였습니다. :: (RetryCount: {})", retryCount, throwable);
    }
}
