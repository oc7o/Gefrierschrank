package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end: JSONL documents (as produced by the dump pipeline) -> Delta table,
 * verified by reading the table back with delta-standalone itself.
 */
class DeltaTableWriterTest {

    @Test
    void repeatedShardTransactionDoesNotDuplicateRows(@TempDir Path target) throws Exception {
        var writer = new DeltaTableWriter(target.toString());
        var conf = new org.apache.hadoop.conf.Configuration();
        assertEquals(1, writer.write(conf, List.of("{\"id\":1}").iterator(), null, "same-shard"));
        assertEquals(0, writer.write(conf, List.of("{\"id\":1}").iterator(), null, "same-shard"));
        var log = io.delta.standalone.DeltaLog.forTable(conf, target.toString());
        assertEquals(0, log.snapshot().getVersion());
    }

    /** A source string beyond Jackson's 20M-char default exports unchanged. */
    @Test
    void exportsSourceWithStringsBeyondJacksonDefaultLimit(@TempDir Path target) throws Exception {
        String payload = "A".repeat(20_000_001);
        String source = "{\"output\":{\"content\":\"" + payload + "\"}}";
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        String row = context.document(source, 0,
            new de.sloow.gefrierschrank.core.lucene.DocumentMetadata("id-1", null, "stored", 0));

        var conf = new org.apache.hadoop.conf.Configuration();
        Path tablePath = target.resolve("out-table");
        assertEquals(1, new DeltaTableWriter(tablePath.toUri().toString())
            .write(conf, List.of(row).iterator(), ArchiveContext.mapping()));
        try (var it = io.delta.standalone.DeltaLog.forTable(conf, tablePath.toString()).snapshot().open()) {
            assertEquals(source, it.next().getString("_source"));
        }
    }

    @Test
    void writesJsonlIntoReadableDeltaTable(@TempDir Path target) throws Exception {
        // 3 test documents; real dumps stream thousands
        List<String> docs = List.of(
            "{\"id\": 1, \"title\": \"Product 1\", \"price\": 9.99}",
            "{\"id\": 2, \"title\": \"Product 2 with \\\"quotes\\\"\", \"price\": 19.5}",
            "{\"id\": 3, \"title\": \"Product 3\", \"price\": null}");

        Path tablePath = target.resolve("out-table");
        new DeltaTableWriter(tablePath.toUri().toString()).write(docs.iterator());

        // read back with delta-standalone: the table exists, rows readable
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        var log = io.delta.standalone.DeltaLog.forTable(conf, tablePath.toString());
        assertTrue(log.tableExists(), "delta log must exist after write");
        long rows = 0;
        try (var it = log.snapshot().open()) {
            while (it.hasNext()) {
                rows++;
                it.next();
            }
        }
        assertEquals(3, rows, "all rows readable from the table scan");
    }

    @Test
    void taggedWriteRecordsShardKeyOnEveryAddFile(@TempDir Path target) throws Exception {
        Path tablePath = target.resolve("out-table");
        var conf = new org.apache.hadoop.conf.Configuration();

        new DeltaTableWriter(tablePath.toUri().toString()).write(
            conf, List.of("{\"id\": 1}", "{\"id\": 2}").iterator(), null, "my_index#0#snap_1");

        var log = io.delta.standalone.DeltaLog.forTable(conf, tablePath.toString());
        var files = log.snapshot().getAllFiles();
        assertFalse(files.isEmpty());
        for (var addFile : files) {
            assertEquals("my_index#0#snap_1", addFile.getTags().get(ShardKey.TAG));
        }
    }

    @Test
    void untaggedWriteLeavesNoShardKeyTag(@TempDir Path target) throws Exception {
        Path tablePath = target.resolve("out-table");
        var conf = new org.apache.hadoop.conf.Configuration();

        // the 3-arg overload (no shardKey) must keep working exactly as before
        new DeltaTableWriter(tablePath.toUri().toString()).write(
            conf, List.of("{\"id\": 1}").iterator(), null);

        var log = io.delta.standalone.DeltaLog.forTable(conf, tablePath.toString());
        for (var addFile : log.snapshot().getAllFiles()) {
            assertNull(addFile.getTags().get(ShardKey.TAG));
        }
    }

    @Test
    void secondWriteToAnExistingTableDoesNotRewriteMetadata(@TempDir Path target) throws Exception {
        Path tablePath = target.resolve("out-table");
        var conf = new org.apache.hadoop.conf.Configuration();
        var writer = new DeltaTableWriter(tablePath.toUri().toString());

        writer.write(conf, List.of("{\"id\": 1}").iterator(), null, "idx#0#snap_1");
        writer.write(conf, List.of("{\"id\": 2}").iterator(), null, "idx#1#snap_1");

        var log = io.delta.standalone.DeltaLog.forTable(conf, tablePath.toString());
        assertEquals(1, log.snapshot().getVersion(), "two commits expected: version 0 then version 1");

        // the second commit's JSON log entry must carry no metaData action - re-asserting an
        // unchanged schema on every shard commit is unnecessary log churn for a table fed by many shards
        Path secondCommitFile = tablePath.resolve("_delta_log").resolve("00000000000000000001.json");
        String secondCommitJson = Files.readString(secondCommitFile);
        assertFalse(secondCommitJson.contains("\"metaData\""),
            "second commit must not re-write metadata for an already-existing table");
    }
    @Test
    void resumedWritesUseTheCommittedPhysicalTypes(@TempDir Path target) throws Exception {
        var writer = new DeltaTableWriter(target.toString());
        writer.write(List.of("{\"value\":1.5}").iterator());
        writer.write(List.of("{\"value\":2}").iterator());
        var log = io.delta.standalone.DeltaLog.forTable(new org.apache.hadoop.conf.Configuration(), target.toString());
        var values = new java.util.HashSet<Double>();
        try (var rows = log.snapshot().open()) {
            while (rows.hasNext()) values.add(rows.next().getDouble("value"));
        }
        assertEquals(java.util.Set.of(1.5, 2.0), values);
    }

    @Test
    void incompatibleValuesDoNotSilentlyBecomeNullOrTruncated(@TempDir Path target) throws Exception {
        var writer = new DeltaTableWriter(target.toString());
        writer.write(List.of("{\"value\":1}").iterator());
        assertThrows(IllegalArgumentException.class,
            () -> writer.write(List.of("{\"value\":2.5}").iterator()));
        var log = io.delta.standalone.DeltaLog.forTable(new org.apache.hadoop.conf.Configuration(), target.toString());
        assertEquals(0, log.snapshot().getVersion());
        assertEquals(1, log.snapshot().getAllFiles().size());
    }

    @Test
    void failedShardRemovesItsUncommittedFiles(@TempDir Path target) throws Exception {
        var documents = new java.util.Iterator<String>() {
            int count;
            public boolean hasNext() { return true; }
            public String next() {
                if (++count > 3) throw new IllegalStateException("failed read");
                return "{\"value\":1}";
            }
        };
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set(DeltaTableWriter.TARGET_FILE_BYTES_KEY, "1");
        assertThrows(IllegalStateException.class, () -> new DeltaTableWriter(target.toString()).write(conf, documents));
        try (var files = Files.list(target)) {
            assertTrue(files.noneMatch(path -> path.toString().endsWith(".parquet")));
        }
        assertFalse(Files.exists(target.resolve("_delta_log")));
    }

    @Test
    void rollsOnCompressedBytes(@TempDir Path target) throws Exception {
        long targetBytes = 4L << 20;
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = logLikeSources(40_000, 44);
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.setLong(DeltaTableWriter.TARGET_FILE_BYTES_KEY, targetBytes);
        String table = target.toString();
        assertEquals(sources.size(), new DeltaTableWriter(table).write(conf, archiveRows(context, sources).iterator(),
            ArchiveContext.mapping(), context.key(0)));

        var files = ParquetFooters.addFiles(conf, table);
        assertTrue(files.size() >= 2, "expected several files, got " + files.size());
        long rowBytes = archiveRows(context, sources).stream().mapToLong(String::length).sum();
        assertTrue(files.size() <= rowBytes / targetBytes / 2,
            files.size() + " files: rolling must count compressed bytes, not the " + rowBytes + " input bytes");
        // parquet-mr counts every open page at its raw size (before dictionary encoding and
        // compression) until it is flushed: up to 1 MiB for _source plus the small columns, whose
        // pages stay open for up to 20k rows. The slack is absolute, i.e. small at the 256 MiB default.
        for (var file : files.subList(0, files.size() - 1)) {
            assertTrue(file.getSize() >= targetBytes - (2L << 20) && file.getSize() <= targetBytes + (2L << 20),
                "file of " + file.getSize() + " bytes for a target of " + targetBytes);
        }
        for (var file : files) assertEquals(context.key(0), file.getTags().get(ShardKey.TAG));
        assertEquals(0, io.delta.standalone.DeltaLog.forTable(conf, table).update().getVersion(), "one commit");
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    @Test
    void largeDocumentsRollBeforeManyRows(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = randomSources(10, 600 << 10, 45);
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set(DeltaTableWriter.TARGET_FILE_BYTES_KEY, "1m");
        String table = target.toString();
        new DeltaTableWriter(table).write(conf, archiveRows(context, sources).iterator(), ArchiveContext.mapping(), context.key(0));

        var footers = ParquetFooters.read(conf, table);
        assertTrue(footers.size() >= 5, "expected >= 5 files, got " + footers.size());
        for (var footer : footers) assertTrue(footer.rows() <= 2, footer.path() + " holds " + footer.rows() + " rows");
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    @Test
    void smallShardProducesOneFile(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        new DeltaTableWriter(target.toString()).write(conf,
            List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}").iterator(), null, "idx#0#snap");
        assertEquals(1, ParquetFooters.addFiles(conf, target.toString()).size());
    }

    @Test
    void repeatedMultiFileShardTransactionDoesNotDuplicateRows(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set(DeltaTableWriter.TARGET_FILE_BYTES_KEY, "1");
        var writer = new DeltaTableWriter(target.toString());
        var docs = List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}");
        assertEquals(3, writer.write(conf, docs.iterator(), null, "same-shard"));
        assertEquals(3, ParquetFooters.addFiles(conf, target.toString()).size());
        assertEquals(0, writer.write(conf, docs.iterator(), null, "same-shard"));
        assertEquals(0, io.delta.standalone.DeltaLog.forTable(conf, target.toString()).update().getVersion());
        assertEquals(3, ParquetFooters.parquetFilesOnDisk(target).size());
    }

    @Test
    void defaultCodecIsZstdAndTheKeySelectsAnother(@TempDir Path target) throws Exception {
        var docs = List.of("{\"id\":1,\"text\":\"a\"}", "{\"id\":2,\"text\":\"b\"}");
        var conf = new org.apache.hadoop.conf.Configuration();
        new DeltaTableWriter(target.resolve("default").toString()).write(conf, docs.iterator());
        assertEquals(java.util.Set.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD),
            ParquetFooters.read(conf, target.resolve("default").toString()).get(0).codecs());

        for (String codec : List.of("snappy", "uncompressed", "ZSTD")) {
            var selected = new org.apache.hadoop.conf.Configuration();
            selected.set(DeltaTableWriter.PARQUET_CODEC_KEY, codec);
            String table = target.resolve("conf-" + codec).toString();
            new DeltaTableWriter(table).write(selected, docs.iterator());
            assertEquals(java.util.Set.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.valueOf(codec.toUpperCase())),
                ParquetFooters.read(selected, table).get(0).codecs(), codec);
        }

        String previous = System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, "snappy");
        try {
            String table = target.resolve("system-property").toString();
            new DeltaTableWriter(table).write(conf, docs.iterator());
            assertEquals(java.util.Set.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY),
                ParquetFooters.read(conf, table).get(0).codecs());
            var explicit = new org.apache.hadoop.conf.Configuration();
            explicit.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
            assertEquals(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
                DeltaTableWriter.parquetCodec(explicit), "the Hadoop conf key wins over the system property");
        } finally {
            if (previous == null) System.clearProperty(DeltaTableWriter.PARQUET_CODEC_KEY);
            else System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, previous);
        }
    }

    @Test
    void zstdRoundTripIsByteExact(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = List.of(
            "{\"ascii\":\"plain\"}",
            "{\"text\":\"Grüße 日本語 \uD83D\uDE00 \\ud83c\\udf89\"}",
            "{\"q\":\"quote \\\" backslash \\\\ slash \\/ tab \\t\"}",
            "{ \"spaced\" :\t[ 1 ,\n 2 ] ,\r\n \"x\" : { } }",
            "{\"big\":12345678901234567890.1234567890123,\"exp\":1E+400,\"neg\":-0.0,\"int\":18446744073709551616}",
            "{\"dup\":1,\"dup\":2,\"order\":{\"z\":1,\"a\":2}}");
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        new DeltaTableWriter(table).write(conf, archiveRows(context, sources).iterator(), ArchiveContext.mapping(), context.key(0));
        assertEquals(java.util.Set.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD),
            ParquetFooters.read(conf, table).get(0).codecs());
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    /** A table may hold shards written with different codecs; resume and the snapshot guard accept it. */
    @Test
    void mixedCodecArchiveResumeKeepsEveryShardExactlyOnce(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        String table = target.toString();
        var uncompressed = new org.apache.hadoop.conf.Configuration();
        uncompressed.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        var zstd = new org.apache.hadoop.conf.Configuration();
        var sources = new java.util.ArrayList<List<String>>();
        for (int shard = 0; shard < 5; shard++) sources.add(randomSources(3, 100, shard));

        var firstRun = new DeltaTableWriter(table);
        for (int shard = 0; shard < 2; shard++)
            firstRun.write(uncompressed, archiveRows(context, sources.get(shard)).iterator(), ArchiveContext.mapping(), context.key(shard));
        var secondRun = new DeltaTableWriter(table);
        secondRun.requireArchiveSnapshot(zstd, context, 5);
        for (int shard = 0; shard < 5; shard++) {
            long written = secondRun.write(zstd, archiveRows(context, sources.get(shard)).iterator(),
                ArchiveContext.mapping(), context.key(shard));
            assertEquals(shard < 2 ? 0 : 3, written, "shard " + shard);
        }
        new DeltaTableWriter(table).requireArchiveSnapshot(zstd, context, 5);

        var codecsByKey = new java.util.HashMap<String, java.util.Set<org.apache.parquet.hadoop.metadata.CompressionCodecName>>();
        for (var file : ParquetFooters.addFiles(zstd, table)) {
            var footer = ParquetFooters.read(zstd, new org.apache.hadoop.fs.Path(table + "/" + file.getPath()));
            assertNull(codecsByKey.put(file.getTags().get(ShardKey.TAG), footer.codecs()), "one file per shard");
        }
        for (int shard = 0; shard < 5; shard++) {
            assertEquals(java.util.Set.of(shard < 2 ? org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
                : org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD), codecsByKey.get(context.key(shard)));
        }
        var expected = new java.util.HashMap<String, Long>();
        for (int shard = 0; shard < 5; shard++) for (String source : sources.get(shard)) expected.merge(source, 1L, Long::sum);
        var actual = new java.util.HashMap<String, Long>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(zstd, table).update().open()) {
            while (rows.hasNext()) actual.merge(rows.next().getString("_source"), 1L, Long::sum);
        }
        assertEquals(expected, actual);
    }

    @Test
    void invalidOutputSettingsAreRejectedBeforeAnyWrite(@TempDir Path target) throws Exception {
        var invalidSettings = new java.util.ArrayList<java.util.Map.Entry<String, String>>();
        for (String invalid : List.of("0", "-1", "abc", "")) invalidSettings.add(java.util.Map.entry(DeltaTableWriter.TARGET_FILE_BYTES_KEY, invalid));
        for (String invalid : List.of("gzip", "lz4", "none", "")) invalidSettings.add(java.util.Map.entry(DeltaTableWriter.PARQUET_CODEC_KEY, invalid));
        for (var setting : invalidSettings) {
            String invalid = setting.toString();
            var conf = new org.apache.hadoop.conf.Configuration();
            conf.set(setting.getKey(), setting.getValue());
            var writer = new DeltaTableWriter(target.toString());
            assertThrows(IllegalArgumentException.class,
                () -> writer.write(conf, List.of("{\"id\":1}").iterator(), null, "idx#0#snap"), invalid);
            assertThrows(IllegalArgumentException.class, () -> writer.committedShardKeys(conf), invalid);
            var context = new ArchiveContext("i", "r", "s", "snap", "i", "s");
            assertThrows(IllegalArgumentException.class, () -> writer.requireArchiveSnapshot(conf, context, 1), invalid);
        }
        assertFalse(Files.exists(target.resolve("_delta_log")));
        assertEquals(java.util.Set.of(), ParquetFooters.parquetFilesOnDisk(target));
    }

    @Test
    void outputFileSizeUsesTheSuppliedFilesystemConfiguration(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.setClass("fs.file.impl", TrackingFileSystem.class, org.apache.hadoop.fs.FileSystem.class);
        conf.setBoolean("fs.file.impl.disable.cache", true);
        TrackingFileSystem.completedParquetStats = 0;
        new DeltaTableWriter(target.toString()).write(conf, List.of("{\"value\":1}").iterator());
        assertTrue(TrackingFileSystem.completedParquetStats > 0,
            "the configured filesystem must stat the completed output, not a fresh default configuration");
    }

    public static class TrackingFileSystem extends org.apache.hadoop.fs.LocalFileSystem {
        static int completedParquetStats;
        @Override
        public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws java.io.IOException {
            var status = super.getFileStatus(path);
            if (path.getName().endsWith(".parquet") && status.getLen() > 0) completedParquetStats++;
            return status;
        }
    }

    /** A shard of multi-MB documents must not buffer them all into one page before the first size check. */
    @Test
    void largeDocumentsAreFlushedPagePerDocument(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = randomSources(12, 3 << 19, 38);
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        assertEquals(12, new DeltaTableWriter(table).write(conf, archiveRows(context, sources).iterator(),
            ArchiveContext.mapping(), context.key(0)));

        int pages = ParquetFooters.read(conf, table).stream().mapToInt(f -> f.pageCount("_source")).sum();
        assertTrue(pages >= 10, "each ~2 MiB value must close its own page, got " + pages + " page(s)");
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    /**
     * Large documents that follow many small ones get a page each too. parquet-mr schedules its next
     * size check from the rows so far, so after a run of small rows it would come hundreds of rows
     * later, and a burst of large documents would wait uncompressed in one page and be compressed
     * all at once, outside any one document's heap reservation.
     */
    @Test
    void largeDocumentsAfterSmallOnesAreFlushedPagePerDocument(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = new java.util.ArrayList<>(randomSources(3000, 560, 40));
        sources.addAll(randomSources(6, 3 << 19, 41));
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        String table = target.toString();
        assertEquals(sources.size(), new DeltaTableWriter(table).write(conf, archiveRows(context, sources).iterator(),
            ArchiveContext.mapping(), context.key(0)));

        long largestPage = 0;
        for (var file : ParquetFooters.addFiles(conf, table)) {
            var input = org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(table + "/" + file.getPath()), conf);
            try (var reader = org.apache.parquet.hadoop.ParquetFileReader.open(input)) {
                for (var block : reader.getFooter().getBlocks()) {
                    for (var column : block.getColumns()) {
                        if (!column.getPath().toDotString().equals("_source")) continue;
                        var pages = reader.readOffsetIndex(column);
                        for (int page = 0; page < pages.getPageCount(); page++) {
                            largestPage = Math.max(largestPage, pages.getCompressedPageSize(page));
                        }
                    }
                }
            }
        }
        assertTrue(largestPage < 3 << 20, "one ~2 MiB value per page at most, got a page of " + largestPage + " bytes");
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    @Test
    void smallDocumentsKeepPageCadence(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = randomSources(20_000, 750, 39);
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        assertEquals(20_000, new DeltaTableWriter(table).write(conf, archiveRows(context, sources).iterator(),
            ArchiveContext.mapping(), context.key(0)));

        long uncompressed = 0;
        int pages = 0;
        long rows = 0;
        for (var footer : ParquetFooters.read(conf, table)) {
            rows += footer.rows();
            for (var group : footer.rowGroups()) {
                uncompressed += group.column("_source").totalUncompressedSize();
                pages += group.column("_source").pageCount();
            }
        }
        assertEquals(20_000, rows);
        // parquet-mr closes a page once it is within 10% of the 1 MiB page size
        long fewest = (uncompressed + (1 << 20) - 1) >> 20;
        long most = (long) Math.ceil(uncompressed / (0.9 * (1 << 20))) + 1;
        assertTrue(pages >= fewest && pages <= most,
            "about one 1 MiB page per MiB of _source: " + pages + " pages for " + uncompressed + " bytes");
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    /** Utf8 values must reach Parquet as exactly the bytes the String path produced. */
    @Test
    void utf8ValuesWriteTheSameParquetBytesAsStrings(@TempDir Path target) throws Exception {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var mapping = new de.sloow.gefrierschrank.core.repo.IndexMapping(mapper.readTree(
            "{\"title\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"},\"obj\":{\"type\":\"object\"}}"));
        List<String> docs = List.of(
            "{\"title\":\"plain\",\"n\":1,\"obj\":{\"a\":[1,2]}}",
            "{\"title\":\"\",\"n\":null,\"obj\":[\"x\",{\"y\":null}]}",
            "{\"title\":null,\"extra\":\"Grüße 日本 \\ud83d\\ude00\"}",
            "{\"title\":\"quotes \\\" and \\\\ backslash\",\"obj\":\"not an object\"}");
        for (var mappingOrNull : java.util.Arrays.asList(mapping, null)) {
            Path table = target.resolve(mappingOrNull == null ? "inferred" : "mapped");
            var conf = new org.apache.hadoop.conf.Configuration();
            assertEquals(docs.size(), new DeltaTableWriter(table.toString()).write(conf, docs.iterator(), mappingOrNull));

            var inferrer = new JsonSchemaMapper();
            if (mappingOrNull != null) inferrer.inferFromEsMapping(mappingOrNull);
            else inferrer.inferFrom(mapper.readTree(docs.get(0)));
            var reference = new org.apache.hadoop.fs.Path(target.resolve("reference.parquet").toUri());
            try (var writer = DeltaTableWriter.parquetWriter(reference, inferrer.avroSchema(), conf,
                    DeltaTableWriter.parquetCodec(conf))) {
                for (String doc : docs) {
                    var node = mapper.readTree(doc);
                    var record = new org.apache.avro.generic.GenericData.Record(inferrer.avroSchema());
                    for (var field : inferrer.avroSchema().getFields()) {
                        record.put(field.name(), field.name().equals(JsonSchemaMapper.UNMAPPED_FIELDS_FIELD)
                            ? inferrer.unmappedFieldsJson(node)
                            : JsonSchemaMapper.avroValue(node.get(inferrer.jsonFieldNameFor(field.name())), field.schema()));
                    }
                    writer.write(record);
                }
            }
            var written = ParquetFooters.addFilePaths(conf, table.toString());
            assertEquals(1, written.size());
            assertArrayEquals(Files.readAllBytes(target.resolve("reference.parquet")),
                Files.readAllBytes(table.resolve(written.iterator().next())));
            Files.delete(target.resolve("reference.parquet"));
        }
    }

    @Test
    void loneSurrogateFailsTheShardWithoutCommittingOrLeavingFiles(@TempDir Path target) throws Exception {
        var mapping = new de.sloow.gefrierschrank.core.repo.IndexMapping(
            new com.fasterxml.jackson.databind.ObjectMapper().readTree("{\"title\":{\"type\":\"keyword\"}}"));
        var conf = new org.apache.hadoop.conf.Configuration();
        assertThrows(RuntimeException.class, () -> new DeltaTableWriter(target.toString()).write(
            conf, List.of("{\"title\":\"ok\"}", "{\"title\":\"\\ud800x\"}").iterator(), mapping, "idx#0#snap"));
        assertFalse(Files.exists(target.resolve("_delta_log")));
        assertEquals(java.util.Set.of(), ParquetFooters.parquetFilesOnDisk(target));
    }

    @Test
    void concurrentCommitsOnAnExistingTableAllLand(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");

        var keys = List.of("idx#1#snap", "idx#2#snap", "idx#3#snap", "idx#4#snap");
        var outcomes = writeConcurrently(i -> writer, conf, keys);
        for (String key : keys) assertEquals(3L, outcomes.get(key), key);
        assertEquals(java.util.Set.of("idx#0#snap", "idx#1#snap", "idx#2#snap", "idx#3#snap", "idx#4#snap"),
            writer.committedShardKeys(conf));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(15, countRows(conf, table));
    }

    @Test
    void concurrentFirstCommitsOnAFreshTableAllLand(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        var keys = List.of("idx#0#snap", "idx#1#snap", "idx#2#snap", "idx#3#snap");
        var outcomes = writeConcurrently(i -> new DeltaTableWriter(table), conf, keys);
        for (String key : keys) assertEquals(3L, outcomes.get(key), key);
        assertEquals(new java.util.HashSet<>(keys), new DeltaTableWriter(table).committedShardKeys(conf));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        try (var commits = Files.list(target.resolve("_delta_log"))) {
            var withMetadata = commits.filter(p -> p.toString().endsWith(".json")).filter(p -> {
                try { return Files.readString(p).contains("\"metaData\""); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            }).map(p -> p.getFileName().toString()).toList();
            assertEquals(List.of("00000000000000000000.json"), withMetadata);
        }
        assertEquals(12, countRows(conf, table));
    }

    @Test
    void sameKeyFromManyThreadsCommitsExactlyOnceAndLosersCleanUp(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(9, 1), null, "idx#9#snap");
        var outcomes = writeConcurrently(i -> i % 2 == 0 ? writer : new DeltaTableWriter(table), conf,
            java.util.Collections.nCopies(4, "idx#0#snap"));
        assertEquals(List.of(0L, 0L, 0L, 3L), outcomes.get("idx#0#snap"));
        assertEquals(1, ParquetFooters.addFiles(conf, table).stream()
            .filter(file -> "idx#0#snap".equals(file.getTags().get(ShardKey.TAG))).count());
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(4, countRows(conf, table));
    }

    @Test
    void commitFailureBeforeLogWriteLeavesNoOrphans(@TempDir Path target) throws Exception {
        var conf = FaultInjectingLogStore.enable(new org.apache.hadoop.conf.Configuration());
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");
        FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.THROW_BEFORE_WRITE, 1);
        try {
            assertThrows(java.io.IOException.class, () -> writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
        } finally {
            FaultInjectingLogStore.reset();
        }
        assertEquals(java.util.Set.of("idx#0#snap"), writer.committedShardKeys(conf));
        writer.reconcilePendingCleanup(conf);
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(3, writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"), "the next attempt commits");
        assertEquals(6, countRows(conf, table));
    }

    @Test
    void commitFailureAfterLogWriteIsReportedCommittedAndKeepsFiles(@TempDir Path target) throws Exception {
        var conf = FaultInjectingLogStore.enable(new org.apache.hadoop.conf.Configuration());
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");
        FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.THROW_AFTER_WRITE, 1);
        try {
            assertEquals(3, writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
        } finally {
            FaultInjectingLogStore.reset();
        }
        writer.reconcilePendingCleanup(conf);
        assertEquals(6, countRows(conf, table), "every committed file is still there");
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(0, writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
    }

    @Test
    void schemaChangedWhileStagingIsNotCommitted(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        var documents = new java.util.Iterator<String>() {
            int next;
            public boolean hasNext() { return next < 3; }
            public String next() {
                if (next == 1) {
                    try {
                        new DeltaTableWriter(table).write(conf, List.of("{\"other\":\"x\"}").iterator(), null, "other#0#snap");
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
                return "{\"value\":" + (next++) + "}";
            }
        };
        assertThrows(java.io.IOException.class, () -> new DeltaTableWriter(table).write(conf, documents, null, "idx#0#snap"));
        var log = io.delta.standalone.DeltaLog.forTable(conf, table);
        assertEquals(0, log.update().getVersion(), "the log holds only the other writer's commit");
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
    }

    @Test
    void conflictingCommitIsRetriedAndLands(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        writer.beforeCommit = () -> {
            if (attempts.getAndIncrement() == 0) writeOtherShard(table, conf, 2);
        };
        assertEquals(3, writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
        assertEquals(2, attempts.get(), "one conflicting attempt, one retry");
        assertEquals(java.util.Set.of("idx#0#snap", "idx#1#snap", "idx#2#snap"), writer.committedShardKeys(conf));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(9, countRows(conf, table));
    }

    @Test
    void conflictOnEveryAttemptDeletesTheStagedFilesAndSurfaces(@TempDir Path target) throws Exception {
        var conf = new org.apache.hadoop.conf.Configuration();
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        writer.beforeCommit = () -> writeOtherShard(table, conf, 100 + attempts.getAndIncrement());
        assertThrows(io.delta.standalone.exceptions.DeltaConcurrentModificationException.class,
            () -> writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
        assertEquals(4, attempts.get());
        assertFalse(writer.committedShardKeys(conf).contains("idx#1#snap"));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
    }

    @Test
    void unreadableLogAfterAFailedCommitKeepsTheFilesUntilItShowsTheOutcome(@TempDir Path target) throws Exception {
        var conf = FaultInjectingLogStore.enable(new org.apache.hadoop.conf.Configuration());
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");
        int committedFiles = ParquetFooters.addFilePaths(conf, table).size();
        writer.beforeCommit = () -> {
            FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.THROW_BEFORE_WRITE, 1);
            FaultInjectingLogStore.failListings();
        };
        try {
            assertThrows(java.io.IOException.class, () -> writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
            writer.reconcilePendingCleanup(conf);
            assertEquals(committedFiles + 1, ParquetFooters.parquetFilesOnDisk(target).size(), "kept while the log is unreadable");
        } finally {
            FaultInjectingLogStore.reset();
        }
        assertEquals(java.util.Set.of("idx#0#snap"), writer.committedShardKeys(conf));
        writer.reconcilePendingCleanup(conf);
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
    }

    @Test
    void failedCommitOfAKeyAnotherWriterCommittedReturnsZeroAndDefersCleanup(@TempDir Path target) throws Exception {
        var conf = FaultInjectingLogStore.enable(new org.apache.hadoop.conf.Configuration());
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        writer.write(conf, shardDocs(0, 3), null, "idx#0#snap");
        writer.beforeCommit = () -> {
            writer.beforeCommit = () -> {};
            writeOtherShard(table, conf, 1);
            FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.THROW_BEFORE_WRITE, 1);
        };
        try {
            assertEquals(0, writer.write(conf, shardDocs(1, 3), null, "idx#1#snap"));
        } finally {
            FaultInjectingLogStore.reset();
        }
        int committedFiles = ParquetFooters.addFilePaths(conf, table).size();
        assertEquals(committedFiles + 1, ParquetFooters.parquetFilesOnDisk(target).size(), "deletion waits for reconcile");
        writer.reconcilePendingCleanup(conf);
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(6, countRows(conf, table));
    }

    /** Commits shard {@code shard} (key idx#shard#snap, 3 rows) through another writer instance. */
    private static void writeOtherShard(String table, org.apache.hadoop.conf.Configuration conf, int shard) {
        try {
            new DeltaTableWriter(table).write(conf, shardDocs(shard, 3), null, "idx#" + shard + "#snap");
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Test
    void aTableNamedThroughASymlinkOrAsAUriSharesItsCommitLock(@TempDir Path target) throws Exception {
        Path real = Files.createDirectories(target.resolve("real"));
        Path link = Files.createSymbolicLink(target.resolve("link"), real);
        String key = DeltaTableWriter.lockKey(real.resolve("table").toString());
        assertEquals(key, DeltaTableWriter.lockKey(link.resolve("table").toString()));
        assertEquals(key, DeltaTableWriter.lockKey(real.resolve("table").toUri().toString()));
        assertEquals(key, DeltaTableWriter.lockKey(real.resolve("x/../table").toString()));
        assertNotEquals(key, DeltaTableWriter.lockKey(real.resolve("other").toString()));
    }

    @Test
    void defaultCodecFallsBackToUncompressedOnlyWhenZstdCannotLoad() {
        assertEquals(org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD, DeltaTableWriter.defaultCodec(null));
        assertEquals(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
            DeltaTableWriter.defaultCodec("UnsatisfiedLinkError"));
        assertEquals(org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD, DeltaTableWriter.defaultParquetCodec());
    }

    /**
     * Where zstd-jni cannot load its native library (noexec java.io.tmpdir), the default codec
     * writes uncompressed Parquet and a configured ZSTD fails before any table work instead of
     * raising an Error mid-shard. A missing -DZstdNativePath file reproduces the load failure.
     */
    @Test
    void withoutTheZstdNativeLibraryTheDefaultWritesUncompressedAndConfiguredZstdIsRejected(@TempDir Path target) throws Exception {
        Path fallback = target.resolve("fallback");
        Path configured = target.resolve("configured");
        Path log = target.resolve("child.log");
        String javaCommand = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process child = new ProcessBuilder(javaCommand, "-DZstdNativePath=" + target.resolve("missing-libzstd-jni.so"),
            "-cp", System.getProperty("java.class.path"), NativeZstdUnavailableChild.class.getName(),
            fallback.toString(), configured.toString())
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(child.waitFor(2, java.util.concurrent.TimeUnit.MINUTES), "child timed out");
        String output = Files.readString(log);
        assertEquals(0, child.exitValue(), output);
        assertEquals(1, output.lines()
            .filter(line -> line.startsWith("warning: Parquet ZSTD compression unavailable (UnsatisfiedLinkError)")).count(), output);
        assertTrue(output.contains("configured zstd: IllegalStateException"), output);

        var conf = new org.apache.hadoop.conf.Configuration();
        assertEquals(java.util.Set.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED),
            ParquetFooters.read(conf, fallback.toString()).get(0).codecs());
        assertEquals(3, countRows(conf, fallback.toString()));
        assertFalse(Files.exists(configured.resolve("_delta_log")));
        assertEquals(java.util.Set.of(), ParquetFooters.parquetFilesOnDisk(Files.createDirectories(configured)));
    }

    /** Child JVM entry point of the test above. */
    public static final class NativeZstdUnavailableChild {
        public static void main(String[] args) throws Exception {
            var conf = new org.apache.hadoop.conf.Configuration();
            new DeltaTableWriter(args[0]).write(conf, shardDocs(0, 3), null, "idx#0#snap");
            var zstd = new org.apache.hadoop.conf.Configuration();
            zstd.set(DeltaTableWriter.PARQUET_CODEC_KEY, "zstd");
            try {
                new DeltaTableWriter(args[1]).write(zstd, shardDocs(0, 3), null, "idx#0#snap");
                System.out.println("configured zstd: written");
            } catch (Exception rejected) {
                System.out.println("configured zstd: " + rejected.getClass().getSimpleName());
            }
        }
    }

    static java.util.Iterator<String> shardDocs(int shard, int rows) {
        var docs = new java.util.ArrayList<String>();
        for (int row = 0; row < rows; row++) docs.add("{\"shard\":" + shard + ",\"row\":" + row + "}");
        return docs.iterator();
    }

    static long countRows(org.apache.hadoop.conf.Configuration conf, String table) throws java.io.IOException {
        long rows = 0;
        var seen = new java.util.HashSet<String>();
        try (var it = io.delta.standalone.DeltaLog.forTable(conf, table).update().open()) {
            while (it.hasNext()) {
                var row = it.next();
                assertTrue(seen.add(row.getLong("shard") + "/" + row.getLong("row")), "duplicate row");
                rows++;
            }
        }
        return rows;
    }

    /**
     * One write per key, in parallel. Every document stream pauses after its first document until
     * all streams are staging, so every commit overlaps every other shard's staging. Returns, per
     * key, the row count (or the sorted row counts when a key repeats), failing on any exception.
     */
    static java.util.Map<String, Object> writeConcurrently(java.util.function.IntFunction<DeltaTableWriter> writers,
                                                          org.apache.hadoop.conf.Configuration conf, List<String> keys) throws Exception {
        var barrier = new java.util.concurrent.CyclicBarrier(keys.size());
        var pool = java.util.concurrent.Executors.newFixedThreadPool(keys.size());
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
            for (int i = 0; i < keys.size(); i++) {
                var writer = writers.apply(i);
                String key = keys.get(i);
                int shard = Integer.parseInt(key.split("#")[1]);
                var documents = new java.util.Iterator<String>() {
                    int next;
                    public boolean hasNext() { return next < 3; }
                    public String next() {
                        if (next == 1) {
                            try {
                                barrier.await(30, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        }
                        return "{\"shard\":" + shard + ",\"row\":" + (next++) + "}";
                    }
                };
                futures.add(pool.submit(() -> writer.write(conf, documents, null, key)));
            }
            var results = new java.util.TreeMap<String, List<Long>>();
            for (int i = 0; i < keys.size(); i++) {
                long rows;
                try {
                    rows = futures.get(i).get(60, java.util.concurrent.TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException failure) {
                    throw new AssertionError("write of " + keys.get(i) + " failed", failure.getCause());
                }
                results.computeIfAbsent(keys.get(i), k -> new java.util.ArrayList<>()).add(rows);
            }
            var outcomes = new java.util.TreeMap<String, Object>();
            results.forEach((key, rows) -> {
                java.util.Collections.sort(rows);
                outcomes.put(key, rows.size() == 1 ? rows.get(0) : rows);
            });
            return outcomes;
        } finally {
            pool.shutdownNow();
        }
    }

    /** Distinct JSON objects whose single string value is base64 of {@code rawBytes} random bytes. */
    static List<String> randomSources(int count, int rawBytes, long seed) {
        var random = new java.util.Random(seed);
        var sources = new java.util.ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            byte[] bytes = new byte[rawBytes];
            random.nextBytes(bytes);
            sources.add("{\"n\":" + i + ",\"blob\":\"" + java.util.Base64.getEncoder().encodeToString(bytes) + "\"}");
        }
        return sources;
    }

    /** Deterministic, log-line-like JSON sources of about 1 KB that compress like real logs. */
    static List<String> logLikeSources(int count, long seed) {
        var random = new java.util.Random(seed);
        String[] words = new String[400];
        for (int i = 0; i < words.length; i++) {
            var word = new StringBuilder();
            for (int c = 3 + random.nextInt(6); c > 0; c--) word.append((char) ('a' + random.nextInt(26)));
            words[i] = word.toString();
        }
        String[] levels = {"INFO", "INFO", "INFO", "DEBUG", "WARN", "ERROR"};
        var sources = new java.util.ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            var message = new StringBuilder();
            for (int w = 60 + random.nextInt(40); w > 0; w--) message.append(words[random.nextInt(words.length)]).append(' ');
            sources.add("{\"@timestamp\":\"2026-09-27T12:" + (10 + i % 50) + ":" + (10 + i % 49) + "." + (100 + i % 900)
                + "Z\",\"log\":{\"level\":\"" + levels[random.nextInt(levels.length)] + "\"},\"service\":{\"name\":\"svc-"
                + random.nextInt(12) + "\"},\"http\":{\"status\":" + (200 + random.nextInt(4) * 100) + ",\"bytes\":"
                + random.nextInt(1_000_000) + "},\"trace\":{\"id\":\"" + Long.toHexString(random.nextLong())
                + Long.toHexString(random.nextLong()) + "\"},\"n\":" + i + ",\"message\":\"" + message.toString().trim() + "\"}");
        }
        return sources;
    }

    static List<String> archiveRows(ArchiveContext context, List<String> sources) throws java.io.IOException {
        var rows = new java.util.ArrayList<String>(sources.size());
        for (int doc = 0; doc < sources.size(); doc++) {
            rows.add(context.document(sources.get(doc), 0,
                new de.sloow.gefrierschrank.core.lucene.DocumentMetadata("id-" + doc, null, "stored", doc)));
        }
        return rows;
    }

    /** Every _source in the table, ordered by the _doc column. */
    static List<String> readSourcesByDoc(org.apache.hadoop.conf.Configuration conf, String table) throws java.io.IOException {
        var byDoc = new java.util.TreeMap<Long, String>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(conf, table).update().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertNull(byDoc.put(row.getLong("_doc"), row.getString("_source")), "duplicate _doc");
            }
        }
        return List.copyOf(byDoc.values());
    }

    /** Files hold row groups of about ROW_GROUP_BYTES, so a larger file has several; rows are unchanged. */
    @Test
    void aFileAboveTheRowGroupSizeHasSeveralRowGroupsAndReadsBackIdentically(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = randomSources(90, 384 << 10, 46);
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        String table = target.toString();
        var rows = new java.util.ArrayList<ArchiveRow>();
        for (int doc = 0; doc < sources.size(); doc++) {
            byte[] bytes = sources.get(doc).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            rows.add(new ArchiveRow(bytes, bytes.length,
                new de.sloow.gefrierschrank.core.lucene.DocumentMetadata("id-" + doc, null, "stored", doc)));
        }
        assertEquals(new DeltaTableWriter.WriteResult(sources.size(), false),
            new DeltaTableWriter(table).writeArchive(conf, List.of(rows.iterator()), context, 0, context.key(0)));

        var footers = ParquetFooters.read(conf, table);
        assertEquals(1, footers.size());
        assertTrue(footers.get(0).size() > DeltaTableWriter.ROW_GROUP_BYTES, "file of " + footers.get(0).size() + " bytes");
        assertTrue(footers.get(0).rowGroups().size() >= 2, footers.get(0).rowGroups().size() + " row group(s)");
        for (var group : footers.get(0).rowGroups()) {
            long bytes = group.chunks().stream().mapToLong(ParquetFooters.Chunk::totalUncompressedSize).sum();
            assertTrue(bytes <= DeltaTableWriter.ROW_GROUP_BYTES + (1 << 20), "row group of " + bytes + " bytes");
        }
        assertEquals(sources, readSourcesByDoc(conf, table));
    }

    @Test
    void emptyMappedShardIsCommittedAndResumable(@TempDir Path target) throws Exception {
        var writer = new DeltaTableWriter(target.toString());
        var conf = new org.apache.hadoop.conf.Configuration();
        var mapping = new de.sloow.gefrierschrank.core.repo.IndexMapping(
            new com.fasterxml.jackson.databind.ObjectMapper().readTree("{\"id\":{\"type\":\"integer\"}}"));
        assertEquals(0, writer.write(conf, List.<String>of().iterator(), mapping, "empty#0#snap"));
        assertTrue(writer.committedShardKeys(conf).contains("empty#0#snap"));
        var log = io.delta.standalone.DeltaLog.forTable(conf, target.toString());
        try (var rows = log.snapshot().open()) { assertFalse(rows.hasNext()); }
        assertEquals(1, writer.write(conf, List.of("{\"id\":7}").iterator(), mapping, "empty#1#snap"));
        try (var rows = log.update().open()) { assertEquals(7, rows.next().getLong("id")); }
    }

}
