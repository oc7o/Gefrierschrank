package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentId;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.lucene10.BenchCodecs;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Single-thread CPU benchmark of Gefrierschrank's stored-source path on a local, already
 * materialized shard (no S3, no download). Compares the production per-document access pattern
 * (Lucene10ShardReader: random-access stored fields, loaded twice per document) with a
 * sequential merge-instance reader (the access pattern ES's fetch phase uses for contiguous
 * doc IDs), both with and without the JSON/Parquet/Delta stage.
 *
 * <p>Usage: Bench <workDir> <docsPerIndex> <secondsPerMeasurement> [variant,variant...]
 *
 * <p>The benchmark lives in the delta package because the archive row path is package-private;
 * {@link ArchiveRows} reaches it reflectively so that jars whose ArchiveContext signature changed
 * still run the read-only measurements. On jars with the archive byte row path ({@link ByteRows}),
 * it also times that production path, with the shard read as one range and as
 * {@link #SHARD_THREADS} ranges.
 *
 * <p>The smile-lz4, cbor-lz4 and yaml-lz4 variants store the same documents in that format (encoded
 * by the jar's own Jackson factories, see {@link StoredFormat}); the reader converts them to JSON, so
 * their throughput is the conversion's. They run only on jars that convert stored formats.
 */
public final class Bench {

    record Variant(String slug, String label, Supplier<Codec> codec, StoredFormat format) {
        Variant(String slug, String label, Supplier<Codec> codec) {
            this(slug, label, codec, StoredFormat.JSON);
        }
    }

    record Result(long docs, long sourceBytes, double seconds) {
        double docsPerSec() { return docs / seconds; }
        double mbPerSec() { return sourceBytes / seconds / 1e6; }
    }

    interface Measurement { Result run(long deadlineNanos) throws Exception; }

    static long blackhole;

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        int docCount = Integer.parseInt(args[1]);
        double budget = Double.parseDouble(args[2]);
        Set<String> only = args.length > 3 ? Set.of(args[3].split(",")) : null;
        List<Variant> variants = List.of(
            new Variant("lz4-80k", "LZ4 80KB (ES <8.14 default)",
                () -> new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED)),
            new Variant("deflate-480k", "DEFLATE 480KB (ES <8.14 best_compression)",
                () -> new Lucene103Codec(Lucene103Codec.Mode.BEST_COMPRESSION)),
            new Variant("zstd-14k", "ZSTD 14KB/128 (ES 8.14+ default)", BenchCodecs.ZstdBestSpeed::new),
            new Variant("zstd-240k", "ZSTD 240KB/2048 (ES 8.14+ best_compression)", BenchCodecs.ZstdBestCompression::new),
            new Variant("smile-lz4", "SMILE _source, LZ4 80KB",
                () -> new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED), StoredFormat.SMILE),
            new Variant("cbor-lz4", "CBOR _source, LZ4 80KB",
                () -> new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED), StoredFormat.CBOR),
            new Variant("yaml-lz4", "YAML _source, LZ4 80KB",
                () -> new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED), StoredFormat.YAML));

        StringBuilder summary = new StringBuilder();
        StringBuilder byteSummary = new StringBuilder(String.format("%-44s | %14s %14s %7s%n",
            "codec", "byte e2e d/s", "K=" + SHARD_THREADS + " e2e d/s", "ratio"));
        summary.append(String.format("%-44s | %12s %12s %7s | %12s %12s %7s | %s%n",
            "codec", "GS read d/s", "seq read d/s", "ratio", "GS e2e d/s", "seq e2e d/s", "ratio", "GS e2e MB/s"));
        for (Variant v : variants) {
            if (only != null && !only.contains(v.slug())) continue;
            if (v.format().unavailableReason() != null) {
                System.out.printf("%n== %s: skipped: %s%n", v.label(), v.format().unavailableReason());
                continue;
            }
            Path dir = root.resolve("index-" + v.slug());
            if (!Files.exists(dir.resolve("done"))) build(dir, v.codec().get(), docCount, v.format());
            long sourceTotal = Long.parseLong(Files.readString(dir.resolve("done")).trim());
            System.out.printf("%n== %s: %d docs, %.1f MB source as stored, stored fields (.fdt) %.1f MB, index %.1f MB%n",
                v.label(), docCount, sourceTotal / 1e6, sizeOf(dir, ".fdt") / 1e6, sizeOf(dir, "") / 1e6);
            if (v.format() != StoredFormat.JSON) {
                // The sequential reader hands out the stored bytes, which are not JSON; the GS paths convert.
                Result gsRead = timed("GS read + conversion (Lucene10ShardReader)", budget, deadline -> gsRead(dir, deadline));
                summary.append(String.format("%-44s | %12.0f %12s %7s | %12s %12s %7s | %s%n",
                    v.label(), gsRead.docsPerSec(), "-", "-", "-", "-", "-", "-"));
                if (!ByteRows.available()) {
                    System.out.println("   byte e2e skipped: " + ByteRows.unavailableReason());
                    continue;
                }
                Path byteTable = root.resolve("delta-" + v.slug() + "-bytes");
                Result byteE2e = timed("GS byte e2e (archive byte row path, MB/s of JSON)", budget,
                    deadline -> ByteRows.pipeline(dir, byteTable, deadline, 1));
                Result rangedE2e = timed("GS byte e2e, " + SHARD_THREADS + " shard threads", budget,
                    deadline -> ByteRows.pipeline(dir, byteTable, deadline, SHARD_THREADS));
                byteSummary.append(String.format("%-44s | %14.0f %14.0f %6.1fx%n", v.label(), byteE2e.docsPerSec(),
                    rangedE2e.docsPerSec(), rangedE2e.docsPerSec() / byteE2e.docsPerSec()));
                continue;
            }

            Result gsRead = timed("GS read (Lucene10ShardReader)", budget, deadline -> gsRead(dir, deadline));
            Result seqRead = timed("sequential merge-instance read", budget, deadline -> seqRead(dir, deadline));
            Path gsTable = root.resolve("delta-" + v.slug() + "-gs");
            Path seqTable = root.resolve("delta-" + v.slug() + "-seq");
            if (!ArchiveRows.available()) {
                System.out.println("   e2e skipped: " + ArchiveRows.unavailableReason());
                summary.append(String.format("%-44s | %12.0f %12.0f %6.1fx | %12s %12s %7s | %s%n",
                    v.label(), gsRead.docsPerSec(), seqRead.docsPerSec(), seqRead.docsPerSec() / gsRead.docsPerSec(),
                    "-", "-", "-", "-"));
                continue;
            }
            Result gsE2e = timed("GS e2e (read+ArchiveContext+DeltaTableWriter)", budget,
                deadline -> pipeline(dir, gsTable, deadline, false));
            System.out.println("   output: " + outputStats(gsTable));
            Result seqE2e = timed("seq read + same JSON/Delta path", budget,
                deadline -> pipeline(dir, seqTable, deadline, true));

            summary.append(String.format("%-44s | %12.0f %12.0f %6.1fx | %12.0f %12.0f %6.1fx | %.1f%n",
                v.label(), gsRead.docsPerSec(), seqRead.docsPerSec(), seqRead.docsPerSec() / gsRead.docsPerSec(),
                gsE2e.docsPerSec(), seqE2e.docsPerSec(), seqE2e.docsPerSec() / gsE2e.docsPerSec(), gsE2e.mbPerSec()));

            if (!ByteRows.available()) {
                System.out.println("   byte e2e skipped: " + ByteRows.unavailableReason());
                continue;
            }
            Path byteTable = root.resolve("delta-" + v.slug() + "-bytes");
            Result byteE2e = timed("GS byte e2e (archive byte row path)", budget,
                deadline -> ByteRows.pipeline(dir, byteTable, deadline, 1));
            Result rangedE2e = timed("GS byte e2e, " + SHARD_THREADS + " shard threads", budget,
                deadline -> ByteRows.pipeline(dir, byteTable, deadline, SHARD_THREADS));
            byteSummary.append(String.format("%-44s | %14.0f %14.0f %6.1fx%n", v.label(), byteE2e.docsPerSec(),
                rangedE2e.docsPerSec(), rangedE2e.docsPerSec() / byteE2e.docsPerSec()));
        }
        System.out.println();
        System.out.print(summary);
        if (ByteRows.available()) {
            System.out.println();
            System.out.print(byteSummary);
        }
        System.out.println("(blackhole " + (blackhole & 1) + ")");
    }

    /** One warm-up run (discarded) followed by the measured run. */
    static Result timed(String label, double budgetSeconds, Measurement m) throws Exception {
        double warmup = Math.min(5, budgetSeconds / 2);
        m.run(System.nanoTime() + (long) (warmup * 1e9));
        Result r = m.run(System.nanoTime() + (long) (budgetSeconds * 1e9));
        System.out.printf("   %-48s %10.0f docs/s  %7.1f MB/s source  (%d docs in %.1fs)%n",
            label, r.docsPerSec(), r.mbPerSec(), r.docs(), r.seconds());
        return r;
    }

    static Result gsRead(Path dir, long deadline) throws IOException {
        long docs = 0, bytes = 0, start = System.nanoTime();
        String[] holder = new String[1];
        try (var reader = new Lucene10ShardReader(dir)) {
            outer:
            while (true) {
                for (int doc = 0; doc < reader.docCount(); doc++) {
                    if (System.nanoTime() > deadline) break outer;
                    if (!reader.isLive(doc)) continue;
                    // Exactly DumpToDeltaPipeline.ShardSourceIterator.next(): readSource, then documentMetadata
                    reader.readSource(doc, (b, len) -> holder[0] = new String(b, 0, len, StandardCharsets.UTF_8));
                    DocumentMetadata metadata = reader.documentMetadata(doc);
                    blackhole += holder[0].length() + metadata.id().length();
                    bytes += holder[0].length();
                    docs++;
                }
            }
        }
        return new Result(docs, bytes, (System.nanoTime() - start) / 1e9);
    }

    static Result seqRead(Path dir, long deadline) throws IOException {
        long docs = 0, bytes = 0, start = System.nanoTime();
        try (Directory directory = new NIOFSDirectory(dir); DirectoryReader reader = DirectoryReader.open(directory)) {
            var visitor = new SourceVisitor();
            outer:
            while (true) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    StoredFieldsReader fields = ((CodecReader) leaf.reader()).getFieldsReader().getMergeInstance();
                    Bits live = leaf.reader().getLiveDocs();
                    for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                        if (System.nanoTime() > deadline) break outer;
                        if (live != null && !live.get(doc)) continue;
                        visitor.reset();
                        fields.document(doc, visitor);
                        String source = new String(visitor.source, StandardCharsets.UTF_8);
                        String id = DocumentId.decode(visitor.id, 0, visitor.id.length);
                        blackhole += source.length() + id.length();
                        bytes += source.length();
                        docs++;
                    }
                }
            }
        }
        return new Result(docs, bytes, (System.nanoTime() - start) / 1e9);
    }

    static Result pipeline(Path dir, Path table, long deadline, boolean sequential) throws Exception {
        deleteRecursively(table);
        var archive = new ArchiveRows();
        var writer = new DeltaTableWriter(table.toString());
        long[] bytes = {0};
        long start = System.nanoTime();
        long rows;
        if (!sequential) {
            try (var reader = new Lucene10ShardReader(dir)) {
                Iterator<String> documents = new Iterator<>() {
                    int next = 0;

                    @Override public boolean hasNext() {
                        if (System.nanoTime() > deadline) return false;
                        while (next < reader.docCount() && !reader.isLive(next)) next++;
                        return next < reader.docCount();
                    }

                    @Override public String next() {
                        var holder = new String[1];
                        try {
                            reader.readSource(next, (b, len) -> holder[0] = new String(b, 0, len, StandardCharsets.UTF_8));
                            bytes[0] += holder[0].length();
                            String row = archive.document(holder[0], 0, reader.documentMetadata(next));
                            next++;
                            return row;
                        } catch (IOException e) { throw new UncheckedIOException(e); }
                    }
                };
                rows = writer.write(new Configuration(), documents, ArchiveContext.mapping(), archive.key(0));
            }
        } else {
            try (Directory directory = new NIOFSDirectory(dir); DirectoryReader reader = DirectoryReader.open(directory)) {
                if (reader.leaves().size() != 1) throw new IllegalStateException("expected a force-merged index");
                LeafReaderContext leaf = reader.leaves().get(0);
                StoredFieldsReader fields = ((CodecReader) leaf.reader()).getFieldsReader().getMergeInstance();
                int maxDoc = leaf.reader().maxDoc();
                var visitor = new SourceVisitor();
                Iterator<String> documents = new Iterator<>() {
                    int next = 0;

                    @Override public boolean hasNext() { return System.nanoTime() <= deadline && next < maxDoc; }

                    @Override public String next() {
                        try {
                            visitor.reset();
                            fields.document(next, visitor);
                            String source = new String(visitor.source, StandardCharsets.UTF_8);
                            bytes[0] += source.length();
                            var metadata = new DocumentMetadata(
                                DocumentId.decode(visitor.id, 0, visitor.id.length), null, "stored", next);
                            String row = archive.document(source, 0, metadata);
                            next++;
                            return row;
                        } catch (IOException e) { throw new UncheckedIOException(e); }
                    }
                };
                rows = writer.write(new Configuration(), documents, ArchiveContext.mapping(), archive.key(0));
            }
        }
        return new Result(rows, bytes[0], (System.nanoTime() - start) / 1e9);
    }

    static final int SHARD_THREADS = 4;

    /**
     * The archive byte row path of jars that have it (ShardRanges.ArchiveRowIterator and
     * DeltaTableWriter.writeArchive), reached reflectively so the bench still compiles against
     * older jars. Ranges are cut and forked as the pipeline does, without its heap bound.
     */
    static final class ByteRows {
        private static final java.lang.reflect.Constructor<?> WHOLE;
        private static final java.lang.reflect.Constructor<?> RANGE;
        private static final java.lang.reflect.Method WRITE;
        private static final java.lang.reflect.Method PLAN;
        private static final java.lang.reflect.Method FORK;
        private static final String PROBLEM;

        static {
            java.lang.reflect.Constructor<?> whole = null, range = null;
            java.lang.reflect.Method write = null, plan = null, fork = null;
            String problem = null;
            try {
                var iterator = Class.forName("de.sloow.gefrierschrank.delta.ShardRanges$ArchiveRowIterator");
                whole = iterator.getDeclaredConstructor(de.sloow.gefrierschrank.core.lucene.ShardReader.class);
                range = iterator.getDeclaredConstructor(de.sloow.gefrierschrank.core.lucene.ShardReader.class,
                    int.class, int.class, java.util.concurrent.atomic.AtomicBoolean.class);
                write = DeltaTableWriter.class.getDeclaredMethod("writeArchive", Configuration.class, List.class,
                    Runnable.class, ArchiveContext.class, int.class, String.class);
                plan = Class.forName("de.sloow.gefrierschrank.delta.ShardRanges").getDeclaredMethod("plan",
                    de.sloow.gefrierschrank.core.lucene.ShardReader.class, int.class, long.class, int.class);
                fork = de.sloow.gefrierschrank.core.lucene.ShardReader.class.getMethod("fork");
                for (var member : new java.lang.reflect.AccessibleObject[]{whole, range, write, plan}) member.setAccessible(true);
            } catch (ReflectiveOperationException | RuntimeException e) {
                problem = "no archive byte row path in this jar (" + e + ")";
            }
            WHOLE = whole;
            RANGE = range;
            WRITE = write;
            PLAN = plan;
            FORK = fork;
            PROBLEM = problem;
        }

        static boolean available() { return PROBLEM == null; }

        static String unavailableReason() { return PROBLEM; }

        static Result pipeline(Path dir, Path table, long deadline, int threads) throws Exception {
            deleteRecursively(table);
            var context = new ArchiveContext("bench-index", "repo-index-id", "snap-uuid", "snap-name", "bench-index", "snap-uuid");
            String previous = System.setProperty("gefrierschrank.shard.threads.minRangeLiveDocs", "1");
            long[] bytes = {0};
            long start = System.nanoTime();
            var readers = new java.util.ArrayList<de.sloow.gefrierschrank.core.lucene.ShardReader>();
            try (var reader = new Lucene10ShardReader(dir)) {
                var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
                var ranges = new java.util.ArrayList<Iterator<?>>();
                if (threads == 1) {
                    ranges.add(until(deadline, (Iterator<?>) WHOLE.newInstance(counting(reader, bytes))));
                } else {
                    Object plan = PLAN.invoke(null, reader, threads, Long.MAX_VALUE, 1);
                    int k = (int) plan.getClass().getMethod("k").invoke(plan);
                    int[] bounds = (int[]) plan.getClass().getMethod("bounds").invoke(plan);
                    readers.add(reader);
                    for (int r = 1; r < k; r++) readers.add((de.sloow.gefrierschrank.core.lucene.ShardReader) FORK.invoke(reader));
                    for (int r = 0; r < k; r++) {
                        ranges.add(until(deadline, (Iterator<?>) RANGE.newInstance(counting(readers.get(r), bytes),
                            bounds[r], bounds[r + 1], cancelled)));
                    }
                }
                Runnable cancel = () -> cancelled.set(true);
                Object result = WRITE.invoke(new DeltaTableWriter(table.toString()), new Configuration(), ranges, cancel,
                    context, 0, context.key(0));
                long rows = (long) result.getClass().getMethod("rows").invoke(result);
                for (int r = readers.size() - 1; r > 0; r--) readers.get(r).close();
                return new Result(rows, bytes[0], (System.nanoTime() - start) / 1e9);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof Exception cause) throw cause;
                throw e;
            } finally {
                if (previous == null) System.clearProperty("gefrierschrank.shard.threads.minRangeLiveDocs");
                else System.setProperty("gefrierschrank.shard.threads.minRangeLiveDocs", previous);
            }
        }

        /** Ends a range at the deadline, which only a benchmark may do. */
        private static Iterator<Object> until(long deadline, Iterator<?> rows) {
            return new Iterator<>() {
                @Override public boolean hasNext() { return System.nanoTime() <= deadline && rows.hasNext(); }
                @Override public Object next() { return rows.next(); }
            };
        }

        /** Counts source bytes; ranges add from several threads, so the count is approximate under contention. */
        private static de.sloow.gefrierschrank.core.lucene.ShardReader counting(
                de.sloow.gefrierschrank.core.lucene.ShardReader reader, long[] bytes) {
            return new de.sloow.gefrierschrank.core.lucene.ShardReader() {
                @Override public String runtimeName() { return reader.runtimeName(); }
                @Override public int docCount() throws IOException { return reader.docCount(); }
                @Override public boolean isLive(int docId) throws IOException { return reader.isLive(docId); }
                @Override public DocumentMetadata documentMetadata(int docId) throws IOException { return reader.documentMetadata(docId); }
                @Override public void readSource(int docId, SourceConsumer consumer) throws IOException {
                    reader.readSource(docId, (b, len) -> {
                        synchronized (bytes) {
                            bytes[0] += len;
                        }
                        consumer.accept(b, len);
                    });
                }
                @Override public void close() throws IOException { reader.close(); }
            };
        }
    }

    static String outputStats(Path table) throws IOException {
        List<Path> files;
        try (var stream = Files.list(table)) {
            files = stream.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
        }
        long total = files.stream().mapToLong(p -> p.toFile().length()).sum();
        @SuppressWarnings("deprecation")
        var footer = org.apache.parquet.hadoop.ParquetFileReader.readFooter(
            new Configuration(), new org.apache.hadoop.fs.Path(files.get(0).toString()));
        var codecs = new TreeSet<String>();
        long rowGroups = footer.getBlocks().size();
        for (var block : footer.getBlocks()) for (var column : block.getColumns()) codecs.add(column.getCodec().name());
        return String.format("%d parquet files, %.1f MB total, avg %.1f MB/file, %d row group(s) in first file, codecs=%s",
            files.size(), total / 1e6, total / 1e6 / files.size(), rowGroups, codecs);
    }

    /** The package-private archive row path of the jar under test, reached reflectively. */
    static final class ArchiveRows {
        private static final Object CONTEXT;
        private static final java.lang.reflect.Method KEY;
        private static final java.lang.reflect.Method DOCUMENT;
        private static final String PROBLEM;

        static {
            Object context = null;
            java.lang.reflect.Method key = null, document = null;
            String problem = null;
            try {
                var ctor = ArchiveContext.class.getDeclaredConstructor(
                    String.class, String.class, String.class, String.class, String.class, String.class);
                context = ctor.newInstance("bench-index", "repo-index-id", "snap-uuid", "snap-name", "bench-index", "snap-uuid");
                key = ArchiveContext.class.getDeclaredMethod("key", int.class);
                document = ArchiveContext.class.getDeclaredMethod("document", String.class, int.class, DocumentMetadata.class);
                key.setAccessible(true);
                document.setAccessible(true);
            } catch (ReflectiveOperationException | RuntimeException e) {
                problem = "ArchiveContext API differs in this jar (" + e + ")";
            }
            CONTEXT = context;
            KEY = key;
            DOCUMENT = document;
            PROBLEM = problem;
        }

        static boolean available() { return PROBLEM == null; }

        static String unavailableReason() { return PROBLEM; }

        String key(int shard) throws IOException { return (String) invoke(KEY, shard); }

        String document(String source, int shard, DocumentMetadata metadata) throws IOException {
            return (String) invoke(DOCUMENT, source, shard, metadata);
        }

        private static Object invoke(java.lang.reflect.Method method, Object... args) throws IOException {
            try {
                return method.invoke(CONTEXT, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof IOException io) throw io;
                throw new IllegalStateException(e.getCause());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static final class SourceVisitor extends StoredFieldVisitor {
        byte[] source;
        byte[] id;

        void reset() { source = null; id = null; }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            return switch (fieldInfo.name) {
                case "_source", "_id" -> Status.YES;
                default -> Status.NO;
            };
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if (fieldInfo.name.equals("_source")) source = value; else id = value;
        }
    }

    static void build(Path dir, Codec codec, int docCount, StoredFormat format) throws IOException {
        deleteRecursively(dir);
        Files.createDirectories(dir);
        var generator = new DocGenerator(42);
        long sourceBytes = 0;
        long started = System.nanoTime();
        try (Directory directory = FSDirectory.open(dir)) {
            var config = new IndexWriterConfig()
                .setCodec(codec)
                .setSoftDeletesField("__soft_deletes")
                .setRAMBufferSizeMB(256)
                .setUseCompoundFile(false);
            try (var writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < docCount; i++) {
                    byte[] source = format.encode(generator.next().getBytes(StandardCharsets.UTF_8));
                    sourceBytes += source.length;
                    Document doc = new Document();
                    doc.add(new StoredField("_id", new BytesRef(generator.idBytes())));
                    doc.add(new StoredField("_source", new BytesRef(source)));
                    doc.add(new NumericDocValuesField("_primary_term", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }
        }
        Files.writeString(dir.resolve("done"), Long.toString(sourceBytes));
        System.out.printf("built %s in %.1fs%n", dir.getFileName(), (System.nanoTime() - started) / 1e9);
    }

    /**
     * The stored format of a variant's _source. SMILE, CBOR and YAML documents are encoded by the
     * jar's Jackson factories, looked up by name because a jar may not contain them.
     */
    enum StoredFormat {
        JSON(null),
        SMILE("com.fasterxml.jackson.dataformat.smile.SmileFactory"),
        CBOR("com.fasterxml.jackson.dataformat.cbor.CBORFactory"),
        YAML("com.fasterxml.jackson.dataformat.yaml.YAMLFactory");

        private static final String CONVERTER = "de.sloow.gefrierschrank.core.source.SourceJsonConverter";
        private final String factoryClass;
        private com.fasterxml.jackson.databind.ObjectMapper writer;

        StoredFormat(String factoryClass) {
            this.factoryClass = factoryClass;
        }

        /** Why this jar cannot run the variant, or null. */
        String unavailableReason() {
            if (factoryClass == null) return null;
            for (String name : new String[]{factoryClass, CONVERTER}) {
                try {
                    Class.forName(name);
                } catch (ClassNotFoundException e) {
                    return name + " is not in this jar";
                }
            }
            return null;
        }

        byte[] encode(byte[] json) throws IOException {
            if (factoryClass == null) return json;
            if (writer == null) {
                try {
                    var factory = (com.fasterxml.jackson.core.JsonFactory) Class.forName(factoryClass).getConstructor().newInstance();
                    writer = new com.fasterxml.jackson.databind.ObjectMapper(factory);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException(e);
                }
            }
            return writer.writeValueAsBytes(new com.fasterxml.jackson.databind.ObjectMapper().readTree(json));
        }
    }

    static long sizeOf(Path dir, String suffix) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> p.toString().endsWith(suffix)).mapToLong(p -> p.toFile().length()).sum();
        }
    }

    static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) Files.delete(p);
        }
    }

    /** Deterministic ECS-style log documents, ~0.9 KB each, with realistic repetition and entropy. */
    static final class DocGenerator {
        private final SplittableRandom random;
        private final String[] words = new String[2000];
        private long timestamp = 1_767_225_600_000L;

        private static final String[] SERVICES = {"checkout", "cart", "catalog", "payment", "shipping", "auth",
            "search", "recommendation", "inventory", "pricing", "notification", "gateway"};
        private static final String[] LEVELS = {"INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "DEBUG",
            "DEBUG", "WARN", "WARN", "ERROR"};
        private static final String[] METHODS = {"GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"};
        private static final int[] STATUSES = {200, 200, 200, 200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500, 503};
        private static final String[] AGENTS = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
            "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
            "okhttp/4.12.0", "curl/8.9.1", "python-requests/2.32.3", "Go-http-client/2.0"};
        private static final String[] COUNTRIES = {"DE", "DE", "DE", "AT", "CH", "FR", "NL", "US", "GB", "PL"};
        private static final String[] CITIES = {"Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Vienna",
            "Zurich", "Paris", "Amsterdam", "London"};

        DocGenerator(long seed) {
            random = new SplittableRandom(seed);
            String[] syllables = {"ka", "lo", "mi", "ne", "ru", "sa", "te", "vo", "zi", "pa", "ro", "de", "an",
                "el", "is", "or", "un", "ba", "ce", "fu"};
            for (int i = 0; i < words.length; i++) {
                StringBuilder w = new StringBuilder();
                int n = 2 + random.nextInt(3);
                for (int s = 0; s < n; s++) w.append(syllables[random.nextInt(syllables.length)]);
                words[i] = w.toString();
            }
        }

        byte[] idBytes() {
            byte[] id = new byte[15];
            random.nextBytes(id);
            if ((id[0] & 255) < 253) return id;
            byte[] escaped = new byte[16];
            escaped[0] = (byte) 253;
            System.arraycopy(id, 0, escaped, 1, 15);
            return escaped;
        }

        private String word() { return words[Math.min(words.length - 1, (int) Math.abs(random.nextGaussian() * 300))]; }

        private String hex(int length) {
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) sb.append(Character.forDigit(random.nextInt(16), 16));
            return sb.toString();
        }

        String next() {
            timestamp += random.nextInt(40);
            String service = SERVICES[random.nextInt(SERVICES.length)];
            String level = LEVELS[random.nextInt(LEVELS.length)];
            int status = STATUSES[random.nextInt(STATUSES.length)];
            int host = random.nextInt(120);
            int country = random.nextInt(COUNTRIES.length);
            StringBuilder message = new StringBuilder();
            switch (random.nextInt(4)) {
                case 0 -> message.append("Processed order ").append(random.nextInt(10_000_000)).append(" in ")
                    .append(random.nextInt(2000)).append(" ms (user=u-").append(random.nextInt(1_000_000))
                    .append(", items=").append(1 + random.nextInt(12)).append(", total=")
                    .append(random.nextInt(900)).append('.').append(10 + random.nextInt(90)).append(" EUR)");
                case 1 -> message.append("Cache miss for key ").append(word()).append(':').append(random.nextInt(100_000))
                    .append(", fetching from upstream ").append(service).append("-backend-").append(random.nextInt(8));
                case 2 -> message.append("Request to /api/v2/").append(word()).append(" failed with status ").append(status)
                    .append(" after ").append(random.nextInt(4)).append(" retries: ");
                default -> { }
            }
            int words = 6 + random.nextInt(18);
            for (int i = 0; i < words; i++) message.append(i == 0 && message.isEmpty() ? "" : " ").append(word());

            return "{\"@timestamp\":\"" + Instant.ofEpochMilli(timestamp) + "\","
                + "\"host\":{\"name\":\"" + service + "-" + host + "\",\"ip\":\"10.1." + (host / 50) + "." + host
                + "\",\"os\":{\"platform\":\"linux\",\"version\":\"6.8.0-" + (40 + host % 7) + "\"}},"
                + "\"service\":{\"name\":\"" + service + "\",\"version\":\"4." + (host % 13) + "." + (host % 5)
                + "\",\"environment\":\"production\"},"
                + "\"log\":{\"level\":\"" + level + "\",\"logger\":\"com.example." + service + "."
                + Character.toUpperCase(word().charAt(0)) + "Service\"},"
                + "\"process\":{\"pid\":" + (1000 + host * 7) + ",\"thread\":{\"name\":\"http-nio-8080-exec-"
                + random.nextInt(200) + "\"}},"
                + "\"http\":{\"request\":{\"method\":\"" + METHODS[random.nextInt(METHODS.length)] + "\",\"bytes\":"
                + random.nextInt(20_000) + "},\"response\":{\"status_code\":" + status + ",\"bytes\":"
                + random.nextInt(200_000) + "}},"
                + "\"url\":{\"path\":\"/api/v2/" + word() + "/" + random.nextInt(1_000_000) + "/" + word()
                + "\",\"domain\":\"shop.example.com\"},"
                + "\"user_agent\":{\"original\":\"" + AGENTS[random.nextInt(AGENTS.length)] + "\"},"
                + "\"client\":{\"ip\":\"" + (1 + random.nextInt(223)) + "." + random.nextInt(256) + "."
                + random.nextInt(256) + "." + random.nextInt(256) + "\",\"geo\":{\"country_iso_code\":\""
                + COUNTRIES[country] + "\",\"city_name\":\"" + CITIES[country] + "\"}},"
                + "\"trace\":{\"id\":\"" + hex(32) + "\"},\"span\":{\"id\":\"" + hex(16) + "\"},"
                + "\"event\":{\"duration\":" + random.nextInt(2_000_000_000) + ",\"dataset\":\"" + service
                + ".app\",\"outcome\":\"" + (status < 400 ? "success" : "failure") + "\"},"
                + "\"labels\":{\"k8s_pod\":\"" + service + "-" + hex(9) + "-" + hex(5) + "\",\"k8s_namespace\":\"shop\"},"
                + "\"message\":\"" + message + "\"}";
        }
    }
}
