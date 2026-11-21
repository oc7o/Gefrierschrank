package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.CommitPointReader;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.SetTransaction;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * A stored-source shard with non-compound segments and every structure selective fetch prunes,
 * served from a repository-like blob directory, plus the in-process Lucene 10 opener with the
 * selective-fetch methods the CLI's runtime opener implements.
 */
final class SelectiveShardFixture {

    static final String PREFIX = "indices/idx/0/";
    static final ObjectMapper JSON = new ObjectMapper();
    static final JsonNode STORED_METADATA = parse("{\"settings\":{},\"mappings\":{\"_doc\":{\"properties\":"
        + "{\"n\":{\"type\":\"long\"},\"text\":{\"type\":\"text\"}}}}}");

    private SelectiveShardFixture() {}

    static JsonNode parse(String json) {
        try {
            return JSON.readTree(json);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    record Shard(Path repo, ShardSnapshot snapshot) {
        /** Bytes of the files that are downloaded rather than embedded in the snapshot metadata. */
        long downloadBytes() {
            return snapshot.files().stream().filter(f -> !f.blobName().startsWith("v__")).mapToLong(RepoFileInfo::length).sum();
        }

        Set<String> blobsOf(java.util.function.Predicate<String> physicalName) {
            return snapshot.files().stream().filter(f -> !f.blobName().startsWith("v__") && physicalName.test(f.physicalName()))
                .map(RepoFileInfo::blobName).collect(Collectors.toSet());
        }

        Set<String> prunableBlobs() {
            return blobsOf(name -> SelectiveFetch.PRUNED_EXTENSIONS.contains(SelectiveFetch.extension(name)));
        }
    }

    static Document root(int id) {
        var doc = new Document();
        doc.add(new StoredField("_id", new BytesRef(("id-" + id).getBytes(StandardCharsets.UTF_8))));
        doc.add(new StringField("_id_terms", "id-" + id, Field.Store.NO));
        if (id % 3 == 0) doc.add(new StoredField("_routing", "r" + id));
        byte[] source = id == 7
            // Not strict UTF-8: the archive path validates such a document on its own and warns once per shard.
            ? new byte[] {'{', '"', 'n', '"', ':', '"', (byte) 0xff, '"', '}'}
            : ("{\"n\":" + id + ",\"text\":\"words " + id + "\"}").getBytes(StandardCharsets.UTF_8);
        doc.add(new StoredField("_source", new BytesRef(source)));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        doc.add(new TextField("text", "words " + id, Field.Store.NO));
        doc.add(new IntPoint("n", id));
        var vectors = new FieldType(StringField.TYPE_NOT_STORED);
        vectors.setStoreTermVectors(true);
        doc.add(new Field("tv", "t" + id, vectors));
        return doc;
    }

    static Document child(int id) {
        var doc = new Document();
        doc.add(new StringField("_id_terms", "id-" + id, Field.Store.NO));
        doc.add(new StringField("_nested_path", "children", Field.Store.NO));
        return doc;
    }

    /** Three non-compound segments: plain roots, nested blocks, soft updates with hard deletes. */
    static Shard storedShard(Path tmp) throws IOException {
        return shard(tmp, writer -> {
            for (int id = 0; id < 60; id++) writer.addDocument(root(id));
            writer.commit();
            for (int id = 60; id < 70; id++) writer.addDocuments(List.of(child(id), child(id), root(id)));
            writer.commit();
            for (int id = 0; id < 60; id += 5) {
                writer.softUpdateDocument(new Term("_id_terms", "id-" + id), root(id + 100),
                    new NumericDocValuesField("__soft_deletes", 1));
            }
            writer.deleteDocuments(new Term("_id_terms", "id-1"), new Term("_id_terms", "id-61"));
        });
    }

    interface Writes {
        void into(IndexWriter writer) throws IOException;
    }

    /** A shard written by {@code writes} into its own repository directory; segments_N and .si are embedded like ES does. */
    static Shard shard(Path tmp, Writes writes) throws IOException {
        Path index = tmp.resolve("index");
        try (var directory = new NIOFSDirectory(index);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setSoftDeletesField("__soft_deletes")
                 .setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE))) {
            writes.into(writer);
            writer.commit();
        }
        Files.deleteIfExists(index.resolve("write.lock"));
        return served(index, tmp.resolve("repo"));
    }

    /** The Lucene shard in {@code index} served from the repository directory {@code repo}; segments_N and .si embedded. */
    static Shard served(Path index, Path repo) throws IOException {
        Path blobs = Files.createDirectories(repo.resolve(PREFIX));
        var files = new ArrayList<RepoFileInfo>();
        int counter = 0;
        try (var directory = new NIOFSDirectory(index); var listing = Files.list(index)) {
            for (Path file : listing.sorted().toList()) {
                String name = file.getFileName().toString();
                String checksum;
                try (var input = directory.openInput(name, IOContext.READONCE)) {
                    checksum = Long.toString(CodecUtil.retrieveChecksum(input), Character.MAX_RADIX);
                }
                byte[] content = Files.readAllBytes(file);
                if (name.startsWith("segments_") || name.endsWith(".si")) {
                    files.add(new RepoFileInfo("v__" + counter++, name, content.length, checksum, "10.3.2",
                        Base64.getEncoder().encodeToString(content)));
                } else {
                    String blob = "__" + counter++;
                    Files.write(blobs.resolve(blob), content);
                    files.add(new RepoFileInfo(blob, name, content.length, checksum, "10.3.2", null));
                }
            }
        }
        long total = files.stream().mapToLong(RepoFileInfo::length).sum();
        return new Shard(repo, new ShardSnapshot("repo", PREFIX, "snap", 1, total, files));
    }

    /** A FileSystemBlobStore counting reads per blob name (shard prefix removed). */
    static final class CountingStore implements BlobStore {
        final FileSystemBlobStore delegate;
        final Map<String, AtomicInteger> reads = new ConcurrentHashMap<>();
        /** Once set, every further read throws it. */
        volatile Error failReads;

        CountingStore(Path root) {
            this.delegate = new FileSystemBlobStore(root);
        }

        private void count(String key) {
            String blob = key.startsWith(PREFIX) ? key.substring(PREFIX.length()) : key;
            reads.computeIfAbsent(blob.replaceAll("\\.part\\d+$", ""), k -> new AtomicInteger()).incrementAndGet();
        }

        @Override public Optional<BlobMetadata> stat(String key) { return delegate.stat(key); }

        @Override public InputStream read(String key) throws IOException {
            if (failReads != null) throw failReads;
            count(key);
            return delegate.read(key);
        }

        @Override public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
            if (failReads != null) throw failReads;
            count(key);
            return delegate.openRange(key, offset, length, expectedVersion);
        }

        @Override public List<String> list(String prefix) { return delegate.list(prefix); }
        @Override public List<BlobMetadata> listWithSizes(String prefix) { return delegate.listWithSizes(prefix); }
        @Override public void put(String key, byte[] content) { delegate.put(key, content); }

        int reads(String blob) {
            AtomicInteger count = reads.get(blob);
            return count == null ? 0 : count.get();
        }

        Set<String> blobsRead() {
            return new TreeSet<>(reads.keySet());
        }
    }

    /** The in-process Lucene 10 opener with the CLI runtime opener's selective-fetch methods, counting its calls. */
    static class Opener implements DumpToDeltaPipeline.ShardReaderOpener {
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger prunedOpens = new AtomicInteger();
        final AtomicInteger commitReads = new AtomicInteger();

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) throws Exception {
            opens.incrementAndGet();
            return new Lucene10ShardReader(shardDir);
        }

        @Override
        public CommitPoint readCommitPoint(ShardSnapshot shard, Path shardDir) throws Exception {
            commitReads.incrementAndGet();
            return CommitPointReader.read(shardDir);
        }

        @Override
        public ShardReader openPruned(ShardSnapshot shard, Path shardDir, JsonNode rawIndexMetadata) throws Exception {
            prunedOpens.incrementAndGet();
            return Lucene10ShardReader.openPruned(shardDir);
        }
    }

    /** A reader whose {@code readSource} fails for the documents {@code failing} accepts. */
    static final class FailingReader implements ShardReader {
        private final ShardReader delegate;
        private final IntPredicate failing;

        FailingReader(ShardReader delegate, IntPredicate failing) {
            this.delegate = delegate;
            this.failing = failing;
        }

        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() throws IOException { return delegate.docCount(); }
        @Override public boolean isLive(int docId) throws IOException { return delegate.isLive(docId); }
        @Override public boolean storedSourceAbsent(int docId) throws IOException { return delegate.storedSourceAbsent(docId); }
        @Override public DocumentMetadata documentMetadata(int docId) throws IOException { return delegate.documentMetadata(docId); }

        @Override
        public void readSource(int docId, SourceConsumer consumer) throws IOException {
            if (failing.test(docId)) throw new InjectedReadFailure();
            delegate.readSource(docId, consumer);
        }

        @Override
        public ShardReader fork() throws IOException {
            ShardReader fork = delegate.fork();
            return fork == null ? null : new FailingReader(fork, failing);
        }

        @Override public void close() throws IOException { delegate.close(); }
    }

    static final class InjectedReadFailure extends IOException {
        InjectedReadFailure() {
            super("injected read failure");
        }
    }

    static DumpToDeltaPipeline.ShardCatalog catalog(ShardSnapshot shard) {
        return new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() { return 1; }
            @Override public ShardSnapshot shardSnapshot(int shardId) { return shard; }
        };
    }

    static ArchiveContext context() {
        return new ArchiveContext("idx", "rid", "snap-uuid", "snap", "idx", "snap-uuid");
    }

    static RunReport export(Shard shard, BlobStore store, String table, Configuration conf,
                            DumpToDeltaPipeline.ShardReaderOpener opener, JsonNode metadata) throws Exception {
        return new DumpToDeltaPipeline(new RepositoryInspector(store), store, table, conf, opener)
            .runSnapshot(context(), metadata, catalog(shard.snapshot()), RunReportListener.NO_OP);
    }

    /** Every column of every row, sorted. */
    static List<String> rows(String table) throws IOException {
        var snapshot = DeltaLog.forTable(new Configuration(), table).update();
        StructField[] fields = snapshot.getMetadata().getSchema().getFields();
        var rows = new ArrayList<String>();
        try (var iterator = snapshot.open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                var line = new StringBuilder();
                for (StructField field : fields) {
                    line.append(field.getName()).append('=');
                    if (row.isNullAt(field.getName())) line.append("null");
                    else if (field.getDataType() instanceof StringType) line.append(row.getString(field.getName()));
                    else if (field.getDataType() instanceof LongType) line.append(row.getLong(field.getName()));
                    else throw new IllegalStateException("unexpected column type of " + field.getName());
                    line.append('\u0001');
                }
                rows.add(line.toString());
            }
        }
        rows.sort(null);
        return rows;
    }

    /** The committed files' tags and the table's transaction ids, file paths left out. */
    static List<String> commitShape(String table) {
        var log = DeltaLog.forTable(new Configuration(), table);
        var shape = new ArrayList<String>();
        for (AddFile file : log.update().getAllFiles()) shape.add("tags=" + new java.util.TreeMap<>(file.getTags()));
        var changes = log.getChanges(0, false);
        while (changes.hasNext()) {
            for (var action : changes.next().getActions()) {
                if (action instanceof SetTransaction txn) shape.add("txn=" + txn.getAppId() + "@" + txn.getVersion());
            }
        }
        shape.sort(null);
        return shape;
    }

    /** Parquet files on disk that the table does not reference (orphans of a failed attempt). */
    static Set<String> unreferencedDataFiles(String table) throws IOException {
        var referenced = DeltaLog.forTable(new Configuration(), table).update().getAllFiles().stream()
            .map(AddFile::getPath).map(p -> Path.of(p).getFileName().toString()).collect(Collectors.toSet());
        try (var files = Files.walk(Path.of(table))) {
            return files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".parquet"))
                .filter(n -> !referenced.contains(n)).collect(Collectors.toSet());
        }
    }
}
