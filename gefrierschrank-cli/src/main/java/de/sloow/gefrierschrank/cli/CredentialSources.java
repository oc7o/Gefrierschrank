package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.FailureDiagnostics;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.S3BlobStore;
import org.apache.hadoop.conf.Configuration;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Credentials per side: the source repository, every backing repository and the target table
 * each read only their own environment namespace or credential source. A session token is never
 * paired with keys from another namespace, and the target never falls through to the source's
 * AWS_* keys.
 *
 * <p>Two modes per side. {@code env} (default) uses static keys from environment variables,
 * which cannot refresh inside a running JVM. {@code ambient} uses refreshing sources without any
 * environment keys: a profile (role_arn, credential_process, SSO), web identity (IRSA), the
 * container endpoint and the instance profile.
 */
final class CredentialSources {

    private CredentialSources() {}

    enum Mode { ENV, AMBIENT }

    /** Picocli converter accepting {@code env} and {@code ambient}. */
    static final class ModeConverter implements CommandLine.ITypeConverter<Mode> {
        @Override
        public Mode convert(String value) {
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "env" -> Mode.ENV;
                case "ambient" -> Mode.AMBIENT;
                default -> throw new CommandLine.TypeConversionException("expected env or ambient");
            };
        }
    }

    /**
     * Misconfigured credential settings. Messages are constant text naming environment variables
     * and options, never their values, so they are safe to print and to put into reports.
     */
    static final class CredentialConfigurationException extends IllegalArgumentException {
        CredentialConfigurationException(String message) {
            super(message);
        }

        CredentialConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** The CredentialConfigurationException in a failure's cause chain, if any. */
    static CredentialConfigurationException configurationFailure(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof CredentialConfigurationException c) return c;
        }
        return null;
    }

    /** Printable description: the fixed message, plus only the class of an underlying cause. */
    static String describe(CredentialConfigurationException failure) {
        Throwable root = FailureDiagnostics.rootCause(failure);
        return failure.getClass().getSimpleName() + ": " + failure.getMessage()
            + (root == failure ? "" : " (caused by " + root.getClass().getName() + ")");
    }

    /** A local directory or an s3:// repository in {@code env} mode, reading unprefixed variables. */
    static BlobStore openSourceStore(String uri, String endpoint, Function<String, String> env) {
        return openSourceStore(uri, endpoint, Mode.ENV, null, env);
    }

    /**
     * A local directory, or an s3://bucket/prefix repository. {@code repositoryPrefix} is the
     * --repository-env prefix of a backing repository (null otherwise); its PREFIX_AWS_* variables
     * then replace the source mode: static PREFIX keys, or the profile named by PREFIX_AWS_PROFILE.
     * The credentials are resolved once before the store is returned, so a misconfiguration fails
     * at startup rather than at the first shard; the result is never printed.
     */
    static BlobStore openSourceStore(String uri, String endpoint, Mode mode, String repositoryPrefix,
                                     Function<String, String> env) {
        if (!uri.startsWith("s3://")) return new FileSystemBlobStore(Path.of(uri));
        var rest = uri.substring("s3://".length());
        int slash = rest.indexOf('/');
        String bucket = slash < 0 ? rest : rest.substring(0, slash);
        String prefix = slash < 0 ? "" : rest.substring(slash + 1);
        Function<String, String> scoped = DumpToDeltaCommand.repositoryEnvironment(repositoryPrefix, env);
        AwsCredentialsProvider provider = sourceProvider(mode, repositoryPrefix != null, scoped);
        preflight(provider);
        return S3BlobStore.s3(endpoint, bucket, prefix, provider, scoped.apply("AWS_REGION"));
    }

    /** A new provider instance per call: stores own and close their provider, so none is shared. */
    static AwsCredentialsProvider sourceProvider(Mode mode, boolean backingRepository, Function<String, String> env) {
        String profile = env.apply("AWS_PROFILE");
        if (backingRepository && profile != null) {
            if (env.apply("AWS_ACCESS_KEY_ID") != null) {
                throw new CredentialConfigurationException(
                    "--repository-env PREFIX sets both PREFIX_AWS_PROFILE and PREFIX_AWS_ACCESS_KEY_ID; set only one");
            }
            return ProfileCredentialsProvider.builder().profileName(profile).build();
        }
        if (mode == Mode.AMBIENT && !backingRepository) {
            if (env.apply("AWS_ACCESS_KEY_ID") != null) {
                throw new CredentialConfigurationException(
                    "--source-credentials ambient cannot be combined with AWS_ACCESS_KEY_ID; unset it or use --source-credentials env");
            }
            return ambientChain();
        }
        DumpToDeltaCommand.Credentials creds = DumpToDeltaCommand.resolveSourceCredentials(env);
        return StaticCredentialsProvider.create(creds.sessionToken() == null
            ? AwsBasicCredentials.create(creds.accessKey(), creds.secretKey())
            : AwsSessionCredentials.create(creds.accessKey(), creds.secretKey(), creds.sessionToken()));
    }

    /**
     * Refreshing sources only, deliberately without the environment and system-property
     * providers, so ambient mode can never pick up static keys meant for another side.
     */
    static List<AwsCredentialsProvider> ambientProviders() {
        return List.of(
            ProfileCredentialsProvider.create(),
            WebIdentityTokenFileCredentialsProvider.create(),
            ContainerCredentialsProvider.builder().build(),
            InstanceProfileCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build());
    }

    private static AwsCredentialsProvider ambientChain() {
        return AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(ambientProviders())
            .build();
    }

    private static void preflight(AwsCredentialsProvider provider) {
        try {
            provider.resolveCredentials();
        } catch (RuntimeException e) {
            if (provider instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw new CredentialConfigurationException("source repository credentials could not be resolved; "
                + "check --source-credentials, AWS_PROFILE or PREFIX_AWS_PROFILE and the role or instance setup", e);
        }
    }

    static final String S3A_PROVIDER = "fs.s3a.aws.credentials.provider";

    /**
     * Target credentials for S3A. The provider is always pinned: S3A's default chain ends in
     * EnvironmentVariableCredentialsProvider, which would silently authenticate the target with
     * the source's AWS_* keys. Ambient mode sets no keys and lists only refreshing sources.
     */
    static void applyTargetCredentials(Configuration conf, Mode mode, Function<String, String> env) {
        if (mode == Mode.AMBIENT) {
            for (String name : List.of("TARGET_AWS_ACCESS_KEY_ID", "TARGET_AWS_SECRET_ACCESS_KEY", "TARGET_AWS_SESSION_TOKEN")) {
                if (env.apply(name) != null) {
                    throw new CredentialConfigurationException("--target-credentials ambient cannot be combined with "
                        + "TARGET_AWS_ACCESS_KEY_ID, TARGET_AWS_SECRET_ACCESS_KEY or TARGET_AWS_SESSION_TOKEN; "
                        + "unset them or use --target-credentials env");
                }
            }
            List<String> providers = new ArrayList<>();
            String profile = env.apply("TARGET_AWS_PROFILE");
            if (profile != null) {
                providers.add("org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider");
                conf.set("fs.s3a.auth.profile.name", profile);
            }
            providers.add("software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider");
            providers.add("org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider");
            conf.set(S3A_PROVIDER, String.join(",", providers));
            return;
        }
        DumpToDeltaCommand.Credentials creds = DumpToDeltaCommand.resolveTargetCredentials(env);
        conf.set("fs.s3a.access.key", creds.accessKey());
        conf.set("fs.s3a.secret.key", creds.secretKey());
        if (creds.sessionToken() != null) {
            conf.set("fs.s3a.session.token", creds.sessionToken());
            conf.set(S3A_PROVIDER, "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        } else {
            conf.set(S3A_PROVIDER, "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        }
    }
}
