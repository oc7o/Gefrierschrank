package de.sloow.gefrierschrank.core.repo;

import java.time.Duration;

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

/**
 * Retry budget of every source-repository S3 client.
 *
 * <p>The SDK's legacy defaults give up after 4 attempts within about half a second and share a
 * 500-token circuit breaker per client, so a short outage drains the bucket and every later
 * request of a long run fails on its first error. This budget is the SDK standard strategy
 * (retrying I/O errors, 5xx, throttling and clock skew; never 403/404) with 10 attempts, full
 * jitter exponential backoff capped at 20 s, no circuit breaker, and Retry-After capped at 20 s:
 * about 92 s of sleep in the worst case, 46 s expected, per call. Measured and simulated, that
 * absorbs about 9 in 10 outages of up to 30 s of refused or reset connections or non-throttling
 * 5xx (backoff from 0.5 s), and of up to 40 s of throttling such as 503 SlowDown (from 1 s).
 *
 * <p>Because the strategy is explicit, the SDK ignores AWS_RETRY_MODE, AWS_MAX_ATTEMPTS and a
 * profile's retry_mode for these clients; {@value S3BlobStore#MAX_ATTEMPTS_PROPERTY} sets the
 * attempt count instead.
 *
 * <p>Timeouts null means unset. The SDK's attempt and call timers stop when a streaming GET
 * returns its body stream, so neither can cut off a long download.
 */
record SourceRetryConfig(int maxAttempts, Duration baseDelay, Duration maxBackoff, Duration throttlingBaseDelay,
                         Duration retryAfterCap, Duration apiCallAttemptTimeout, Duration apiCallTimeout) {

    static final SourceRetryConfig DEFAULT = new SourceRetryConfig(10, Duration.ofMillis(500), Duration.ofSeconds(20),
        Duration.ofSeconds(1), Duration.ofSeconds(20), Duration.ofMinutes(2), Duration.ofMinutes(10));

    /**
     * DEFAULT with the attempt count from {@value S3BlobStore#MAX_ATTEMPTS_PROPERTY} (at least 1).
     * Above 10 attempts the overall call timeout grows to 1 min per attempt, DEFAULT's ratio of
     * 10 min for 10 attempts. That covers attempts failing on the 30 s socket timeout plus 20 s
     * waits; as with DEFAULT, attempts that stall up to the 2 min attempt timeout can still hit
     * the call timeout first.
     */
    static SourceRetryConfig configured() {
        int attempts = Math.max(1, Integer.getInteger(S3BlobStore.MAX_ATTEMPTS_PROPERTY, DEFAULT.maxAttempts));
        if (attempts == DEFAULT.maxAttempts) return DEFAULT;
        Duration callTimeout = attempts <= DEFAULT.maxAttempts ? DEFAULT.apiCallTimeout : Duration.ofMinutes(attempts);
        return new SourceRetryConfig(attempts, DEFAULT.baseDelay, DEFAULT.maxBackoff, DEFAULT.throttlingBaseDelay,
            DEFAULT.retryAfterCap, DEFAULT.apiCallAttemptTimeout, callTimeout);
    }

    RetryStrategy retryStrategy() {
        var standard = AwsRetryStrategy.standardRetryStrategy().toBuilder()
            .maxAttempts(maxAttempts)
            .backoffStrategy(BackoffStrategy.exponentialDelay(baseDelay, maxBackoff))
            .throttlingBackoffStrategy(BackoffStrategy.exponentialDelay(throttlingBaseDelay, maxBackoff))
            .circuitBreakerEnabled(false)
            .build();
        return new CappedServerDelayRetryStrategy(standard, retryAfterCap);
    }
}
