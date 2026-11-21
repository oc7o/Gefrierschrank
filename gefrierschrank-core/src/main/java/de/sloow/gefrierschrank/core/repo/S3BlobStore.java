package de.sloow.gefrierschrank.core.repo;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * BlobStore over an S3-compatible object store (AWS S3, MinIO) with an optional
 * key prefix standing in for the repository root.
 *
 * <p>Every client built here shares one configuration: the {@link SourceRetryConfig#DEFAULT}
 * retry budget (attempts adjustable by {@value #MAX_ATTEMPTS_PROPERTY}) and an Apache HTTP pool
 * sized by {@value #MAX_CONNECTIONS_PROPERTY} (default {@value #DEFAULT_MAX_CONNECTIONS}, enough
 * for 8 shard workers with 8 download streams each). The store is safe for concurrent reads.
 *
 * <p>The SDK retries a GET only until it hands the body stream over. A body that then breaks
 * (connection reset, premature close, read timeout) is resumed here: the bytes not yet delivered
 * are requested again as a range pinned to the ETag of the first response, so the caller sees
 * every byte exactly once and in order, and never bytes of two object versions (see
 * {@link #MAX_BODY_RESUMES}).
 */
public final class S3BlobStore implements BlobStore {

    public static final String MAX_CONNECTIONS_PROPERTY = "gefrierschrank.s3.maxConnections";
    static final int DEFAULT_MAX_CONNECTIONS = 128;
    /** Attempts per source request, default 10; see {@link SourceRetryConfig}. */
    public static final String MAX_ATTEMPTS_PROPERTY = "gefrierschrank.s3.maxAttempts";
    /**
     * Reopens per GET body after a mid-body failure. Each reopen is a fresh request with the
     * SDK's own retry budget, after a jittered pause of 0.2 s doubling up to 5 s, so a store that
     * stays down still fails the read within minutes.
     */
    public static final int MAX_BODY_RESUMES = 5;

    /** Bound and pacing of the mid-body resume; a test seam, production uses {@link #DEFAULT}. */
    record BodyResume(int maxResumes, Duration baseDelay, Duration maxDelay) {
        static final BodyResume DEFAULT = new BodyResume(MAX_BODY_RESUMES, Duration.ofMillis(200), Duration.ofSeconds(5));
    }

    private final S3Client client;
    private final String bucket;
    private final String prefix;
    private final AwsCredentialsProvider ownedCredentials;
    private final BodyResume bodyResume;
    private final AtomicLong bodyResumes = new AtomicLong();

    public S3BlobStore(S3Client client, String bucket, String prefix) {
        this(client, bucket, prefix, null, BodyResume.DEFAULT);
    }

    private S3BlobStore(S3Client client, String bucket, String prefix, AwsCredentialsProvider ownedCredentials,
                        BodyResume bodyResume) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = normalize(prefix);
        this.ownedCredentials = ownedCredentials;
        this.bodyResume = bodyResume;
    }

    public static S3BlobStore minio(String endpoint, String bucket, String prefix,
                                    String accessKey, String secretKey) {
        return s3(endpoint, bucket, prefix, accessKey, secretKey, null, "us-east-1");
    }

    public static S3BlobStore s3(String endpoint, String bucket, String prefix,
                                String accessKey, String secretKey, String sessionToken, String region) {
        AwsCredentials credentials = sessionToken == null
            ? AwsBasicCredentials.create(accessKey, secretKey)
            : AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
        return s3(endpoint, bucket, prefix, StaticCredentialsProvider.create(credentials), region, SourceRetryConfig.configured());
    }

    /**
     * A store authenticated by {@code credentials}, resolved per request so refreshing providers
     * (STS roles, web identity, instance profiles) keep a long run signed. The store owns the
     * provider and closes it after its client; never share one provider between stores.
     */
    public static S3BlobStore s3(String endpoint, String bucket, String prefix, AwsCredentialsProvider credentials,
                                String region) {
        return s3(endpoint, bucket, prefix, credentials, region, SourceRetryConfig.configured());
    }

    /** Test seam: the production client with an injected retry budget. */
    static S3BlobStore s3(String endpoint, String bucket, String prefix, AwsCredentialsProvider credentials,
                          String region, SourceRetryConfig retry) {
        return s3(endpoint, bucket, prefix, credentials, region, retry, BodyResume.DEFAULT, null);
    }

    /** Test seam: also the body resume pacing, and a socket timeout (null: the SDK's 30 s). */
    static S3BlobStore s3(String endpoint, String bucket, String prefix, AwsCredentialsProvider credentials,
                          String region, SourceRetryConfig retry, BodyResume bodyResume, Duration socketTimeout) {
        var overrides = ClientOverrideConfiguration.builder().retryStrategy(retry.retryStrategy());
        if (retry.apiCallAttemptTimeout() != null) overrides.apiCallAttemptTimeout(retry.apiCallAttemptTimeout());
        if (retry.apiCallTimeout() != null) overrides.apiCallTimeout(retry.apiCallTimeout());
        var http = ApacheHttpClient.builder()
            .maxConnections(Integer.getInteger(MAX_CONNECTIONS_PROPERTY, DEFAULT_MAX_CONNECTIONS));
        if (socketTimeout != null) http.socketTimeout(socketTimeout);
        var builder = S3Client.builder()
            .region(Region.of(region == null ? "us-east-1" : region))
            .credentialsProvider(credentials)
            .httpClientBuilder(http)
            .overrideConfiguration(overrides.build())
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());
        if (endpoint != null) builder.endpointOverride(URI.create(endpoint));
        S3Client client;
        try {
            client = builder.build();
        } catch (RuntimeException | Error e) {
            closeQuietly(credentials, e);
            throw e;
        }
        return new S3BlobStore(client, bucket, prefix, credentials, bodyResume);
    }

    private static void closeQuietly(AwsCredentialsProvider credentials, Throwable failure) {
        if (credentials instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                failure.addSuppressed(e);
            }
        }
    }

    private static String normalize(String prefix) {
        if (prefix == null || prefix.isEmpty() || prefix.equals("/")) return "";
        return prefix.replaceAll("^/+", "").replaceAll("/+$", "") + "/";
    }

    @Override
    public Optional<BlobMetadata> stat(String key) {
        try {
            var head = client.headObject(b -> b.bucket(bucket).key(prefix + key));
            return Optional.of(new BlobMetadata(key, head.contentLength()));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        }
    }

    /**
     * The whole blob. With a strong ETag and a known length the body resumes after a mid-body
     * failure; otherwise it fails as the connection does.
     */
    @Override
    public InputStream read(String key) throws IOException {
        ResponseInputStream<GetObjectResponse> response;
        try {
            response = client.getObject(b -> b.bucket(bucket).key(prefix + key), ResponseTransformer.toInputStream());
        } catch (NoSuchKeyException e) {
            throw new IOException("Blob not found: " + key, e);
        }
        var body = new AbortOnEarlyClose(response);
        String eTag = strongETag(response.response().eTag());
        Long length = response.response().contentLength();
        if (eTag == null || length == null || length <= 0) return body;
        return new ResumingBody(key, eTag, 0, length, length, body);
    }

    /**
     * Pinned to one object version by ETag: If-Match once the version is known, and the ETag of
     * every response is compared as well, for stores that ignore If-Match.
     *
     * <p>An unpinned first range from a store that cannot pin (it ignores Range, sends no ETag,
     * or only a weak one, which If-Match never matches) still returns the requested bytes, but
     * with a null version, so callers read the blob whole as 1.0.0 did instead of failing on
     * every later range.
     */
    @Override
    public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
        OpenedRange range = openRangeBody(key, offset, length, expectedVersion);
        InputStream body = range.version() == null ? range.body()
            : new ResumingBody(key, range.version(), offset, length, range.objectLength(), range.body());
        return new RangeRead(new ExactLengthInputStream(body, length), range.version(), range.objectLength());
    }

    private record OpenedRange(AbortOnEarlyClose body, String version, long objectLength) {}

    /**
     * One ranged GET, validated, with the raw body: when the store ignored Range it runs past the
     * range, and it is never resumed.
     */
    private OpenedRange openRangeBody(String key, long offset, long length, String expectedVersion) throws IOException {
        BlobStore.requireValidRange(offset, length);
        ResponseInputStream<GetObjectResponse> response;
        try {
            response = client.getObject(b -> {
                b.bucket(bucket).key(prefix + key).range("bytes=" + offset + "-" + (offset + length - 1));
                if (expectedVersion != null) b.ifMatch(expectedVersion);
            }, ResponseTransformer.toInputStream());
        } catch (NoSuchKeyException e) {
            throw new IOException("Blob not found: " + key, e);
        } catch (S3Exception e) {
            if (e.statusCode() == 412) throw new IOException(BLOB_CHANGED, e);
            if (e.statusCode() == 416) throw new IOException(RANGE_OUTSIDE_BLOB, e);
            throw e;
        }
        GetObjectResponse metadata = response.response();
        long[] contentRange = parseContentRange(metadata.contentRange());
        if (contentRange == null && expectedVersion == null && metadata.sdkHttpResponse().statusCode() == 200) {
            return wholeObjectRange(response, offset, length);
        }
        String failure = null;
        if (contentRange == null || contentRange[0] != offset) failure = RANGE_NOT_HONOURED;
        else if (contentRange[1] != offset + length - 1 || contentRange[2] < offset + length) failure = RANGE_OUTSIDE_BLOB;
        else if (expectedVersion != null && metadata.eTag() == null) failure = "object store returned no ETag for a ranged read";
        else if (expectedVersion != null && !expectedVersion.equals(metadata.eTag())) failure = BLOB_CHANGED;
        if (failure != null) {
            response.abort();
            response.close();
            throw new IOException(failure);
        }
        return new OpenedRange(new AbortOnEarlyClose(response), strongETag(metadata.eTag()), contentRange[2]);
    }

    private static final String RANGE_NOT_HONOURED = "object store did not honour the byte range";

    private static String strongETag(String eTag) {
        return eTag == null || eTag.startsWith("W/") ? null : eTag;
    }

    /** The store ignored Range and is sending the whole object: the range is cut from it, unpinned. */
    private static OpenedRange wholeObjectRange(ResponseInputStream<GetObjectResponse> response, long offset, long length)
            throws IOException {
        Long objectLength = response.response().contentLength();
        var in = new AbortOnEarlyClose(response);
        try {
            if (objectLength != null && offset + length > objectLength) throw new IOException(RANGE_OUTSIDE_BLOB);
            in.skipNBytes(offset);
        } catch (EOFException e) {
            in.close();
            throw new IOException(RANGE_OUTSIDE_BLOB, e);
        } catch (IOException | RuntimeException e) {
            in.close();
            throw e;
        }
        return new OpenedRange(in, null, objectLength == null ? -1 : objectLength);
    }

    /** {@code bytes first-last/total} as {first, last, total}; null when absent or unparseable. */
    static long[] parseContentRange(String header) {
        if (header == null) return null;
        var matcher = CONTENT_RANGE.matcher(header.trim());
        if (!matcher.matches()) return null;
        return new long[]{Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)), Long.parseLong(matcher.group(3))};
    }

    private static final java.util.regex.Pattern CONTENT_RANGE =
        java.util.regex.Pattern.compile("bytes (\\d{1,18})-(\\d{1,18})/(\\d{1,18})");

    /**
     * Closing an HTTP body before its end would otherwise drain the remainder to reuse the
     * connection, which for a failed or cancelled multi-GB download means reading gigabytes for
     * nothing. A body read to its end is closed normally so the connection is reused.
     */
    private static final class AbortOnEarlyClose extends FilterInputStream {
        private final ResponseInputStream<GetObjectResponse> response;
        private final long contentLength;
        // close() may come from the thread cancelling a download while another thread reads
        private volatile long consumed;
        private volatile boolean ended;

        AbortOnEarlyClose(ResponseInputStream<GetObjectResponse> response) {
            super(response);
            this.response = response;
            Long length = response.response().contentLength();
            this.contentLength = length == null ? -1 : length;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b < 0) ended = true;
            else consumed++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int count = super.read(b, off, len);
            if (count < 0) ended = true;
            else consumed += count;
            return count;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = super.skip(n);
            consumed += skipped;
            return skipped;
        }

        @Override
        public void close() throws IOException {
            if (!ended && consumed != contentLength) response.abort();
            super.close();
        }

        /** Drops the connection even when the body ended: a body that ended early left it unusable. */
        void abortAndClose() throws IOException {
            response.abort();
            super.close();
        }
    }

    /** How many times a broken body of this store was reopened; for tests. */
    long bodyResumes() {
        return bodyResumes.get();
    }

    /**
     * {@code length} bytes of one blob version from blob offset {@code start}. When the current
     * response fails before its end (an IOException from the connection, or an end of stream
     * before {@code length} bytes), the rest is requested again from the first byte not yet
     * returned, with If-Match on the pinned ETag, and the response must continue exactly there:
     * same Content-Range start, same object length, same ETag. A blob that changed in between
     * fails with {@link BlobStore#BLOB_CHANGED}; bytes of two versions are never combined.
     *
     * <p>Not resumed: a failure after {@link #close} (cancellation), an interrupt, or more than
     * {@link BodyResume#maxResumes} failures of one body. {@link #close} may come from another
     * thread at any time and wins over a pending reopen: nothing is requested after it, and a
     * response opened concurrently is closed again.
     */
    private final class ResumingBody extends InputStream {
        private final String key;
        private final String eTag;
        private final long start;
        private final long length;
        private final long objectLength;
        private final CountDownLatch closedSignal = new CountDownLatch(1);
        private final byte[] single = new byte[1];
        private volatile AbortOnEarlyClose current;
        private volatile boolean closed;
        private long delivered;
        private int resumes;

        ResumingBody(String key, String eTag, long start, long length, long objectLength, AbortOnEarlyClose first) {
            this.key = key;
            this.eTag = eTag;
            this.start = start;
            this.length = length;
            this.objectLength = objectLength;
            this.current = first;
        }

        @Override
        public int read() throws IOException {
            int count;
            do count = read(single, 0, 1); while (count == 0);
            return count < 0 ? -1 : single[0] & 0xFF;
        }

        /** 0 when the current response cannot tell: the next read resumes or reports the failure. */
        @Override
        public int available() {
            if (closed) return 0;
            try {
                return current.available();
            } catch (IOException e) {
                return 0;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            if (len == 0) return 0;
            while (true) {
                IOException failure;
                boolean endedEarly = false;
                try {
                    int count = current.read(b, off, len);
                    if (count > 0) {
                        delivered += count;
                        return count;
                    }
                    if (count == 0) return 0;
                    if (delivered >= length) return -1;
                    endedEarly = true;
                    failure = new EOFException("GET body ended early");
                } catch (IOException e) {
                    failure = e;
                }
                if (!resumable(failure)) {
                    // An early end that is not resumed stays an early end, as before resuming existed.
                    if (endedEarly) return -1;
                    throw failure;
                }
                resume(failure);
            }
        }

        private boolean resumable(IOException failure) {
            if (closed || Thread.currentThread().isInterrupted()) return false;
            if (failure instanceof ClosedByInterruptException) return false;
            // A read timeout is an InterruptedIOException, but it means a stalled connection, not an interrupt.
            if (failure instanceof InterruptedIOException && !(failure instanceof SocketTimeoutException)) return false;
            return delivered < length && resumes < bodyResume.maxResumes();
        }

        private void resume(IOException failure) throws IOException {
            resumes++;
            bodyResumes.incrementAndGet();
            System.err.println("Resuming the source download of " + key + " after " + failure.getClass().getSimpleName()
                + " (" + resumes + "/" + bodyResume.maxResumes() + ")");
            try {
                current.abortAndClose();
            } catch (IOException | RuntimeException e) {
                failure.addSuppressed(e);
            }
            pause(failure);
            AbortOnEarlyClose next = reopen(failure);
            current = next;
            if (closed) {
                closeQuietly(next);
                throw failure;
            }
        }

        private void pause(IOException failure) throws IOException {
            long ceiling = Math.min(bodyResume.maxDelay().toMillis(), bodyResume.baseDelay().toMillis() << (resumes - 1));
            long millis = ThreadLocalRandom.current().nextLong(ceiling / 2, ceiling + 1);
            try {
                if (closedSignal.await(millis, TimeUnit.MILLISECONDS)) throw failure;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw failure;
            }
            if (closed || Thread.currentThread().isInterrupted()) throw failure;
        }

        private AbortOnEarlyClose reopen(IOException failure) throws IOException {
            long from = start + delivered;
            OpenedRange next;
            try {
                next = openRangeBody(key, from, length - delivered, eTag);
            } catch (IOException e) {
                String message = e.getMessage();
                IOException terminal = BLOB_CHANGED.equals(message) || RANGE_OUTSIDE_BLOB.equals(message)
                    || (message != null && message.startsWith("Blob not found"))
                    ? new IOException(BLOB_CHANGED, e) : e;
                terminal.addSuppressed(failure);
                throw terminal;
            } catch (RuntimeException e) {
                e.addSuppressed(failure);
                throw e;
            }
            if (next.objectLength() != objectLength || !eTag.equals(next.version())) {
                closeQuietly(next.body());
                var changed = new IOException(BLOB_CHANGED);
                changed.addSuppressed(failure);
                throw changed;
            }
            return next.body();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            closedSignal.countDown();
            current.close();
        }
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (IOException | RuntimeException ignored) {
            // the body is abandoned; its connection is aborted, not reused
        }
    }

    @Override
    public List<String> list(String prefixDir) {
        var results = new java.util.ArrayList<String>();
        client.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(this.prefix + prefixDir))
            .contents()
            .forEach((S3Object obj) -> {
                String key = obj.key().substring(this.prefix.length());
                results.add(key);
            });
        return results;
    }

    /**
     * One ListObjectsV2Paginator sweep (1000 keys/page from the server, transparently paged by
     * the SDK) - the same call list() makes, just also keeping the size that every page already
     * carries per object instead of discarding it. No HEAD per key: on a large repository that
     * would be one round trip per object instead of one LIST page per thousand, the difference
     * between minutes and (depending on request-rate limits) plausibly hours. Still materializes
     * one BlobMetadata per key in memory (~150 bytes/record), acceptable for a one-shot diagnostic
     * command; a streaming {@code Iterator<BlobMetadata>} variant would be the next step for very
     * large listings.
     */
    @Override
    public List<BlobMetadata> listWithSizes(String prefixDir) {
        var results = new java.util.ArrayList<BlobMetadata>();
        client.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(this.prefix + prefixDir))
            .contents()
            .forEach((S3Object obj) -> {
                String key = obj.key().substring(this.prefix.length());
                results.add(new BlobMetadata(key, obj.size()));
            });
        return results;
    }

    public void put(String key, byte[] content) {
        client.putObject(b -> b.bucket(bucket).key(prefix + key),
            software.amazon.awssdk.core.sync.RequestBody.fromBytes(content));
    }

    @Override
    public void close() {
        try {
            client.close();
        } finally {
            if (ownedCredentials instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    // a provider that fails to release its refresh thread must not fail the run
                }
            }
        }
    }
}
