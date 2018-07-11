package com.outbrain.aletheia.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retry policy, the enforcement of the policy is up to caller.
 */
public class RetryPolicy {

    private static final Logger logger = LoggerFactory.getLogger(RetryPolicy.class);

    private final int maxRetries;

    private int delayMillis;

    public RetryPolicy(final int maxRetries, final int delayMillis) {
        this.maxRetries = maxRetries;
        this.delayMillis = delayMillis;
    }

    /**
     * waits for pre defined in CTOR time.
     */
    public void delay() {
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            logger.error("delay was interrupted", e);
        }
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getDelayMillis() {
        return delayMillis;
    }
}
