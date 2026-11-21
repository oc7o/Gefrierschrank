# Contributing

## Build

Requires JDK 21+ and Maven.

```sh
mvn install
```

This builds all modules and produces the runnable CLI jar at
`gefrierschrank-cli/target/gefrierschrank-cli-1.2.0.jar`, which reads every shard
in-process.

## Tests

```sh
./run-tests.sh   # equivalent to: mvn package -DskipTests && mvn test
```

The S3 tests (`S3BlobStoreTest`, `S3RangedReadTest`, `S3BodyResumeMinioTest`,
`StsCredentialsS3Test`, `InspectCommandsS3Test`, `DeltaS3WriterTest`) need a local MinIO
instance on port 9000. They are regular JUnit tests, not hidden behind a profile, so start
MinIO before running the full suite:

```sh
docker compose up -d minio
```

## Pull requests

- Small fixes: open a PR directly.
- Anything larger (new source format, new export target, changes to the
  `BlobStore` seam or the quarantine/resume behaviour): open an issue first and
  describe the approach, so we can agree on it before you invest the time.
- Keep the functional-core/imperative-shell split (see "Development notes" in the
  README) and add tests against the fixture repository where possible.
- Note the license split in the README: `gefrierschrank-synthetic-source` is
  AGPL-3.0, with its vendored x-pack mappers under the Elastic License 2.0 only;
  everything else is Apache-2.0.
