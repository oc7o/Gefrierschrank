package de.sloow.gefrierschrank.core.repo;

import java.time.Duration;

import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RecordSuccessRequest;
import software.amazon.awssdk.retries.api.RecordSuccessResponse;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;

/**
 * Caps a server-suggested retry delay (Retry-After) and delegates everything else.
 *
 * <p>The SDK waits max(Retry-After, computed backoff) with no upper bound, so an S3-compatible
 * server answering throttled requests with "Retry-After: 60" stalls the serial pipeline for a
 * minute per attempt. The delegate must be a fully defaulted strategy (AwsRetryStrategy): this
 * wrapper is not a DefaultAwareRetryStrategy, so the client builder adds no retry conditions to
 * it. {@link #toBuilder()} returns the delegate's builder; a rebuilt strategy merely loses the cap.
 */
final class CappedServerDelayRetryStrategy implements RetryStrategy {

    private final RetryStrategy delegate;
    private final Duration cap;

    CappedServerDelayRetryStrategy(RetryStrategy delegate, Duration cap) {
        this.delegate = delegate;
        this.cap = cap;
    }

    @Override
    public AcquireInitialTokenResponse acquireInitialToken(AcquireInitialTokenRequest request) {
        return delegate.acquireInitialToken(request);
    }

    @Override
    public RefreshRetryTokenResponse refreshRetryToken(RefreshRetryTokenRequest request) {
        if (request.suggestedDelay().filter(delay -> delay.compareTo(cap) > 0).isPresent()) {
            request = request.toBuilder().suggestedDelay(cap).build();
        }
        return delegate.refreshRetryToken(request);
    }

    @Override
    public RecordSuccessResponse recordSuccess(RecordSuccessRequest request) {
        return delegate.recordSuccess(request);
    }

    @Override
    public int maxAttempts() {
        return delegate.maxAttempts();
    }

    @Override
    public boolean useClientDefaults() {
        return false;
    }

    @Override
    public Builder<?, ?> toBuilder() {
        return delegate.toBuilder();
    }
}
