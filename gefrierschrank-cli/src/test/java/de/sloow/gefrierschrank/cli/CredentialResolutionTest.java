package de.sloow.gefrierschrank.cli;

import org.apache.hadoop.fs.s3a.impl.ConfigureShadedAWSSocketFactory;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Which credentials each side (source, backing repository, target) picks up from the environment. */
class CredentialResolutionTest {

    /** Sentinel values that must never show up in an exception message. */
    static final String SECRET = "sentinel-secret-value";
    static final String TOKEN = "sentinel-token-value";
    static final String KEY = "sentinel-access-key";

    static Map<String, String> env(String... pairs) {
        var env = new HashMap<String, String>();
        for (int i = 0; i < pairs.length; i += 2) env.put(pairs[i], pairs[i + 1]);
        return env;
    }

    @Test
    void sourceTokenPairsOnlyWithTheSourceKeys() {
        var withToken = DumpToDeltaCommand.resolveSourceCredentials(
            env("AWS_ACCESS_KEY_ID", "k", "AWS_SECRET_ACCESS_KEY", "s", "AWS_SESSION_TOKEN", "t")::get);
        assertEquals("t", withToken.sessionToken());
        var withoutToken = DumpToDeltaCommand.resolveSourceCredentials(
            env("AWS_ACCESS_KEY_ID", "k", "AWS_SECRET_ACCESS_KEY", "s", "TARGET_AWS_SESSION_TOKEN", "target")::get);
        assertNull(withoutToken.sessionToken());
        var blankToken = DumpToDeltaCommand.resolveSourceCredentials(
            env("AWS_ACCESS_KEY_ID", "k", "AWS_SECRET_ACCESS_KEY", "s", "AWS_SESSION_TOKEN", " ")::get);
        assertNull(blankToken.sessionToken());
    }

    @Test
    void backingRepositoryTokensStayInTheirPrefix() {
        var env = env("AWS_ACCESS_KEY_ID", "primary", "AWS_SECRET_ACCESS_KEY", "primary-secret", "AWS_SESSION_TOKEN", "primary-token",
            "FROZEN_AWS_ACCESS_KEY_ID", "backing", "FROZEN_AWS_SECRET_ACCESS_KEY", "backing-secret");
        var backing = DumpToDeltaCommand.resolveSourceCredentials(DumpToDeltaCommand.repositoryEnvironment("FROZEN", env::get));
        assertEquals("backing", backing.accessKey());
        assertNull(backing.sessionToken(), "the primary's token must not pair with the backing repository's keys");
        env.put("FROZEN_AWS_SESSION_TOKEN", "backing-token");
        assertEquals("backing-token", DumpToDeltaCommand.resolveSourceCredentials(
            DumpToDeltaCommand.repositoryEnvironment("FROZEN", env::get)).sessionToken());
        assertEquals("primary-token", DumpToDeltaCommand.resolveSourceCredentials(env::get).sessionToken());
    }

    @Test
    void targetTokenPairingMatrix() {
        var targetKeysAndToken = DumpToDeltaCommand.resolveTargetCredentials(env(
            "TARGET_AWS_ACCESS_KEY_ID", "tk", "TARGET_AWS_SECRET_ACCESS_KEY", "ts", "TARGET_AWS_SESSION_TOKEN", "tt",
            "AWS_ACCESS_KEY_ID", "k", "AWS_SECRET_ACCESS_KEY", "s", "AWS_SESSION_TOKEN", "t")::get);
        assertEquals(List.of("tk", "ts", "tt"), List.of(targetKeysAndToken.accessKey(), targetKeysAndToken.secretKey(),
            targetKeysAndToken.sessionToken()));

        var targetKeysSourceToken = DumpToDeltaCommand.resolveTargetCredentials(env(
            "TARGET_AWS_ACCESS_KEY_ID", "tk", "TARGET_AWS_SECRET_ACCESS_KEY", "ts", "AWS_SESSION_TOKEN", "t")::get);
        assertEquals("tk", targetKeysSourceToken.accessKey());
        assertNull(targetKeysSourceToken.sessionToken());

        var fallback = DumpToDeltaCommand.resolveTargetCredentials(env(
            "AWS_ACCESS_KEY_ID", "k", "AWS_SECRET_ACCESS_KEY", "s", "AWS_SESSION_TOKEN", "t")::get);
        assertEquals(List.of("k", "s", "t"), List.of(fallback.accessKey(), fallback.secretKey(), fallback.sessionToken()));
    }

    @Test
    void targetTokenWithoutTargetKeysFailsInsteadOfFallingBack() {
        for (var env : List.of(
            env("TARGET_AWS_SESSION_TOKEN", TOKEN, "AWS_ACCESS_KEY_ID", KEY, "AWS_SECRET_ACCESS_KEY", SECRET),
            env("TARGET_AWS_SESSION_TOKEN", TOKEN, "TARGET_AWS_ACCESS_KEY_ID", KEY, "AWS_ACCESS_KEY_ID", KEY,
                "AWS_SECRET_ACCESS_KEY", SECRET))) {
            var failure = assertThrows(CredentialSources.CredentialConfigurationException.class,
                () -> DumpToDeltaCommand.resolveTargetCredentials(env::get));
            assertMessageFreeOfSentinels(failure);
        }
    }

    @Test
    void missingCredentialsFailWithMessagesThatNameVariablesOnly() {
        assertMessageFreeOfSentinels(assertThrows(CredentialSources.CredentialConfigurationException.class,
            () -> DumpToDeltaCommand.resolveSourceCredentials(env("AWS_ACCESS_KEY_ID", KEY, "AWS_SESSION_TOKEN", TOKEN)::get)));
        assertMessageFreeOfSentinels(assertThrows(CredentialSources.CredentialConfigurationException.class,
            () -> DumpToDeltaCommand.resolveTargetCredentials(env("TARGET_AWS_ACCESS_KEY_ID", KEY, "AWS_SECRET_ACCESS_KEY", SECRET)::get)));
    }

    @Test
    void s3aConfPinsTemporaryCredentialsWhenATokenIsSet() {
        var conf = DumpToDeltaCommand.s3aConf("s3a://bucket/table", "http://target:9000", env(
            "TARGET_AWS_ACCESS_KEY_ID", "tk", "TARGET_AWS_SECRET_ACCESS_KEY", "ts", "TARGET_AWS_SESSION_TOKEN", "tt")::get);
        assertEquals("tk", conf.get("fs.s3a.access.key"));
        assertEquals("ts", conf.get("fs.s3a.secret.key"));
        assertEquals("tt", conf.get("fs.s3a.session.token"));
        assertEquals("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider", conf.get("fs.s3a.aws.credentials.provider"));
        assertEquals("http://target:9000", conf.get("fs.s3a.endpoint"));
    }

    @Test
    void s3aConfPinsSimpleCredentialsWithoutAToken() {
        var conf = DumpToDeltaCommand.s3aConf("s3a://bucket/table", "http://target:9000", env(
            "TARGET_AWS_ACCESS_KEY_ID", "tk", "TARGET_AWS_SECRET_ACCESS_KEY", "ts", "AWS_SESSION_TOKEN", "source-token",
            "AWS_ACCESS_KEY_ID", "source-key", "AWS_SECRET_ACCESS_KEY", "source-secret")::get);
        assertEquals("tk", conf.get("fs.s3a.access.key"));
        assertNull(conf.get("fs.s3a.session.token"));
        assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider", conf.get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    void localTablesGetNoS3aCredentials() {
        var conf = DumpToDeltaCommand.s3aConf("/tmp/table", null, env(
            "AWS_ACCESS_KEY_ID", KEY, "AWS_SECRET_ACCESS_KEY", SECRET, "AWS_SESSION_TOKEN", TOKEN)::get);
        assertNull(conf.get("fs.s3a.access.key"));
        assertNull(conf.get("fs.s3a.secret.key"));
        assertNull(conf.get("fs.s3a.session.token"));
        assertNull(conf.get("fs.s3a.endpoint"));
    }

    /**
     * The fat jar keeps the first copy of each class in this module's dependency order, which is
     * also this classpath's order. The SDK bundle that hadoop-aws ships must supply the Apache
     * client too: a stray apache-client of another SDK version ahead of it mixes SDK versions,
     * and S3A's TLS socket-factory binding then throws a NoSuchMethodError that S3A only logs at
     * debug level, silently ignoring fs.s3a.ssl.channel.mode.
     */
    @Test
    void theSdkHttpClientComesFromTheSameSdkBuildAsS3a() throws Exception {
        var sdkJars = Stream.of(ApacheHttpClient.class, SdkHttpClient.class, SdkBytes.class,
                AwsRetryStrategy.class, S3Client.class)
            .map(type -> type.getProtectionDomain().getCodeSource().getLocation())
            .distinct().toList();
        assertEquals(1, sdkJars.size(), "SDK classes load from " + sdkJars);
        DelegatingSSLSocketFactory.initializeDefaultFactory(DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE);
        new ConfigureShadedAWSSocketFactory().configureSocketFactory(
            ApacheHttpClient.builder(), DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE);
    }

    @Test
    void credentialFailuresAreReportedWithTheirFixedMessage() {
        assumeTrue(System.getenv("AWS_ACCESS_KEY_ID") == null || System.getenv("AWS_SECRET_ACCESS_KEY") == null,
            "needs an environment without source AWS keys");
        var err = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        int exit;
        try {
            exit = new CommandLine(new DumpToDeltaCommand()).execute("s3://bucket/repo", "--index", "i", "-o", "/nonexistent/table");
        } finally {
            System.setErr(original);
        }
        assertEquals(1, exit);
        assertTrue(err.toString(StandardCharsets.UTF_8).contains(
            "error: CredentialConfigurationException: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"), err.toString(StandardCharsets.UTF_8));
    }

    @Test
    void targetAmbientModeSetsNoKeysAndNeverListsTheEnvironmentProvider() {
        var conf = DumpToDeltaCommand.s3aConf("s3a://bucket/table", "http://target:9000", CredentialSources.Mode.AMBIENT,
            env("AWS_ACCESS_KEY_ID", KEY, "AWS_SECRET_ACCESS_KEY", SECRET, "AWS_SESSION_TOKEN", TOKEN)::get);
        assertNull(conf.get("fs.s3a.access.key"));
        assertNull(conf.get("fs.s3a.secret.key"));
        assertNull(conf.get("fs.s3a.session.token"));
        assertNull(conf.get("fs.s3a.auth.profile.name"));
        assertEquals("software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider,"
            + "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider", conf.get("fs.s3a.aws.credentials.provider"));
        assertFalse(conf.get("fs.s3a.aws.credentials.provider").contains("EnvironmentVariable"));

        var profiled = DumpToDeltaCommand.s3aConf("s3a://bucket/table", "http://target:9000", CredentialSources.Mode.AMBIENT,
            env("TARGET_AWS_PROFILE", "lake-writer", "AWS_PROFILE", "source-profile")::get);
        assertEquals("lake-writer", profiled.get("fs.s3a.auth.profile.name"));
        assertTrue(profiled.get("fs.s3a.aws.credentials.provider")
            .startsWith("org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider,"));
    }

    @Test
    void ambientModesRejectExplicitKeysOfTheirSide() {
        for (String name : List.of("TARGET_AWS_ACCESS_KEY_ID", "TARGET_AWS_SECRET_ACCESS_KEY", "TARGET_AWS_SESSION_TOKEN")) {
            var failure = assertThrows(CredentialSources.CredentialConfigurationException.class, () -> DumpToDeltaCommand.s3aConf(
                "s3a://bucket/table", "http://target:9000", CredentialSources.Mode.AMBIENT, env(name, SECRET)::get));
            assertMessageFreeOfSentinels(failure);
        }
        var source = assertThrows(CredentialSources.CredentialConfigurationException.class, () -> CredentialSources.sourceProvider(
            CredentialSources.Mode.AMBIENT, false, env("AWS_ACCESS_KEY_ID", KEY, "AWS_SECRET_ACCESS_KEY", SECRET)::get));
        assertMessageFreeOfSentinels(source);
    }

    @Test
    void sourceAmbientChainHoldsOnlyRefreshingProviders() throws Exception {
        var providers = CredentialSources.ambientProviders();
        try {
            assertEquals(List.of("ProfileCredentialsProvider", "WebIdentityTokenFileCredentialsProvider",
                "ContainerCredentialsProvider", "InstanceProfileCredentialsProvider"),
                providers.stream().map(p -> p.getClass().getSimpleName()).toList());
        } finally {
            for (var provider : providers) if (provider instanceof AutoCloseable c) c.close();
        }
    }

    @Test
    void backingRepositoryProfileYieldsAProfileProviderForThatRepositoryOnly() throws Exception {
        var env = env("AWS_ACCESS_KEY_ID", "primary", "AWS_SECRET_ACCESS_KEY", "primary-secret",
            "FROZEN_AWS_PROFILE", "frozen-reader", "WARM_AWS_ACCESS_KEY_ID", "warm", "WARM_AWS_SECRET_ACCESS_KEY", "warm-secret",
            "WARM_AWS_SESSION_TOKEN", "warm-token");
        var frozen = CredentialSources.sourceProvider(CredentialSources.Mode.ENV, true,
            DumpToDeltaCommand.repositoryEnvironment("FROZEN", env::get));
        assertInstanceOf(ProfileCredentialsProvider.class, frozen);
        ((AutoCloseable) frozen).close();

        var warm = CredentialSources.sourceProvider(CredentialSources.Mode.AMBIENT, true,
            DumpToDeltaCommand.repositoryEnvironment("WARM", env::get)).resolveCredentials();
        assertEquals("warm", warm.accessKeyId());
        assertEquals("warm-token", ((AwsSessionCredentials) warm).sessionToken());

        var primary = CredentialSources.sourceProvider(CredentialSources.Mode.ENV, false, env::get).resolveCredentials();
        assertEquals("primary", primary.accessKeyId());
        assertInstanceOf(AwsBasicCredentials.class, primary);

        env.put("FROZEN_AWS_ACCESS_KEY_ID", KEY);
        assertMessageFreeOfSentinels(assertThrows(CredentialSources.CredentialConfigurationException.class,
            () -> CredentialSources.sourceProvider(CredentialSources.Mode.ENV, true,
                DumpToDeltaCommand.repositoryEnvironment("FROZEN", env::get))));
    }

    /**
     * A profile whose credential_process issues short-lived keys is re-resolved in-process, with
     * no environment change: the refresh path for runs longer than a token's lifetime.
     */
    @Test
    void profileCredentialsRefreshWithoutEnvironmentChanges(@TempDir Path tmp) throws Exception {
        Path script = tmp.resolve("creds.sh");
        Files.writeString(script, String.join("\n",
            "#!/bin/sh",
            "dir=$(dirname \"$0\")",
            "n=$(cat \"$dir/counter\" 2>/dev/null || echo 0); n=$((n+1)); echo $n > \"$dir/counter\"",
            "later=$(( $(date +%s) + 1 ))",
            "exp=$(date -u -d \"@$later\" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r \"$later\" +%Y-%m-%dT%H:%M:%SZ)",
            "printf '{\"Version\":1,\"AccessKeyId\":\"process-key-%s\",\"SecretAccessKey\":\"s\",\"SessionToken\":\"t\",\"Expiration\":\"%s\"}' \"$n\" \"$exp\"",
            ""));
        Path config = tmp.resolve("config");
        Files.writeString(config, "[profile rotating]\ncredential_process = /bin/sh " + script + "\n"
            + "[default]\ncredential_process = /bin/sh " + script + "\n");
        Path credentials = Files.writeString(tmp.resolve("credentials"), "");
        var previous = new HashMap<String, String>();
        for (String property : List.of("aws.configFile", "aws.sharedCredentialsFile", "aws.profile")) {
            previous.put(property, System.getProperty(property));
        }
        System.setProperty("aws.configFile", config.toString());
        System.setProperty("aws.sharedCredentialsFile", credentials.toString());
        System.setProperty("aws.profile", "default");
        try {
            var backing = CredentialSources.sourceProvider(CredentialSources.Mode.ENV, true,
                DumpToDeltaCommand.repositoryEnvironment("FROZEN", env("FROZEN_AWS_PROFILE", "rotating")::get));
            String first = backing.resolveCredentials().accessKeyId();
            Thread.sleep(1100);
            String second = backing.resolveCredentials().accessKeyId();
            assertTrue(first.startsWith("process-key-") && second.startsWith("process-key-"), first + " " + second);
            assertNotEquals(first, second);
            ((AutoCloseable) backing).close();

            var ambient = CredentialSources.sourceProvider(CredentialSources.Mode.AMBIENT, false, env()::get);
            String ambientFirst = ambient.resolveCredentials().accessKeyId();
            Thread.sleep(1100);
            assertNotEquals(ambientFirst, ambient.resolveCredentials().accessKeyId());
            ((AutoCloseable) ambient).close();
        } finally {
            previous.forEach((property, value) -> {
                if (value == null) System.clearProperty(property);
                else System.setProperty(property, value);
            });
        }
    }

    @Test
    void credentialModeOptionAcceptsEnvAndAmbientOnly(@TempDir Path tmp) {
        var converter = new CredentialSources.ModeConverter();
        assertEquals(CredentialSources.Mode.ENV, converter.convert("env"));
        assertEquals(CredentialSources.Mode.AMBIENT, converter.convert("AMBIENT"));
        assertEquals(2, new CommandLine(new DumpToDeltaCommand()).execute(tmp.toString(), "--index", "i",
            "-o", tmp.resolve("t").toString(), "--source-credentials", "static"));
        assertEquals(2, new CommandLine(new DumpToDeltaCommand()).execute(tmp.toString(), "--index", "i",
            "-o", tmp.resolve("t").toString(), "--target-credentials", "keys"));
    }

    static void assertMessageFreeOfSentinels(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String message = String.valueOf(t.getMessage());
            for (String sentinel : List.of(SECRET, TOKEN, KEY)) {
                assertFalse(message.contains(sentinel), "message leaks a credential value: " + message);
            }
        }
    }
}
