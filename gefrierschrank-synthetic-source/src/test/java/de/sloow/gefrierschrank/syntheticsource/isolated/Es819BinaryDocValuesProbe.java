package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Test helper that must be defined by an isolated class loader (hence this package): its
 * Lucene/Elasticsearch references then resolve to that loader's ES classes, including its
 * {@code NativeAccessHolder}. Called reflectively; every method takes and returns JDK types only.
 */
public final class Es819BinaryDocValuesProbe {

    private Es819BinaryDocValuesProbe() {}

    /** The production bootstrap, including its zstd guard. */
    public static void bootstrap() {
        IsolatedEsBootstrap.ensure();
    }

    /** Logging only, for loaders without the NativeAccessHolder substitution (negative control). */
    public static void initLoggingOnly() {
        org.elasticsearch.logging.internal.spi.LoggerFactory.setInstance(
            new org.elasticsearch.common.logging.internal.LoggerFactoryImpl());
    }

    /** Facts about {@code NativeAccess.instance()}, plus every getter that differs from NoopNativeAccess. */
    public static List<String> nativeAccessFacts() throws Exception {
        NativeAccess instance = NativeAccess.instance();
        Class<?> noopClass = Class.forName("org.elasticsearch.nativeaccess.NoopNativeAccess");
        Constructor<?> constructor = noopClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        NativeAccess noop = (NativeAccess) constructor.newInstance();
        List<String> facts = new ArrayList<>();
        facts.add("class=" + instance.getClass().getName());
        facts.add("definedByThisLoader=" + (instance.getClass().getClassLoader() == Es819BinaryDocValuesProbe.class.getClassLoader()));
        facts.add("holderLoader=" + (Class.forName("org.elasticsearch.nativeaccess.NativeAccessHolder", false,
            Es819BinaryDocValuesProbe.class.getClassLoader()).getClassLoader() == Es819BinaryDocValuesProbe.class.getClassLoader()));
        facts.add("zstd=" + (instance.getZstd() != null));
        facts.add("vectorSimilarityFunctions=" + instance.getVectorSimilarityFunctions().isPresent());
        facts.add("execSandboxState=" + instance.getExecSandboxState());
        facts.add("definitelyRunningAsRoot=" + instance.definitelyRunningAsRoot());
        facts.add("isMemoryLocked=" + instance.isMemoryLocked());
        Path here = Path.of(".");
        record Getter(String name, Object isolated, Object noop) {}
        for (Getter getter : List.of(
            new Getter("definitelyRunningAsRoot", instance.definitelyRunningAsRoot(), noop.definitelyRunningAsRoot()),
            new Getter("isMemoryLocked", instance.isMemoryLocked(), noop.isMemoryLocked()),
            new Getter("getExecSandboxState", instance.getExecSandboxState(), noop.getExecSandboxState()),
            new Getter("getProcessLimits", instance.getProcessLimits(), noop.getProcessLimits()),
            new Getter("getVectorSimilarityFunctions", instance.getVectorSimilarityFunctions(), noop.getVectorSimilarityFunctions()),
            new Getter("allocatedSizeInBytes", instance.allocatedSizeInBytes(here), noop.allocatedSizeInBytes(here)))) {
            if (!String.valueOf(getter.isolated()).equals(String.valueOf(getter.noop()))) {
                facts.add("differsFromNoop=" + getter.name());
            }
        }
        return facts;
    }

    /** ES's own ZstdCompressionMode compressor output for {@code data}: VInt length plus one frame. */
    public static byte[] esCompress(byte[] data, int level) throws IOException {
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (Compressor compressor = new ZstdCompressionMode(level).newCompressor()) {
            compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), out);
        }
        return out.toArrayCopy();
    }

    /** ES's own ZstdCompressionMode decompressor: the {@code [offset, offset+length)} window of the block. */
    public static byte[] esDecompress(byte[] block, int originalLength, int offset, int length) throws IOException {
        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        BytesRef bytes = new BytesRef();
        decompressor.decompress(new ByteArrayDataInput(block), originalLength, offset, length, bytes);
        return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
    }

    /** {@code field=format} for every BINARY doc-values field of a materialized shard, in segment order. */
    public static List<String> binaryDocValuesFormats(Path shardDir) throws IOException {
        List<String> formats = new ArrayList<>();
        try (Directory directory = new NIOFSDirectory(shardDir); DirectoryReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                for (FieldInfo field : leaf.reader().getFieldInfos()) {
                    if (field.getDocValuesType() == DocValuesType.BINARY) {
                        formats.add(field.name + "=" + field.getAttribute("PerFieldDocValuesFormat.format"));
                    }
                }
            }
        }
        return formats;
    }

    static String url(int doc) {
        if (doc % 11 == 5) return "";
        Random random = new Random(doc);
        StringBuilder value = new StringBuilder("/api/v").append(doc % 5).append('/');
        int length = doc % 997 == 13 ? 150_000 : random.nextInt(doc % 97 == 0 ? 400 : 60);
        for (int i = 0; i < length; i++) value.append((char) ('a' + random.nextInt(26)));
        return value.toString();
    }

    static boolean hasUrl(int doc) {
        return doc % 7 != 3;
    }

    /** Writes one segment whose BINARY, NUMERIC and SORTED_SET doc values use ES819 with ZSTD binary blocks. */
    public static void write(Path dir, int docs) throws IOException {
        DocValuesFormat es819 = new ES819TSDBDocValuesFormat(BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1);
        IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false).setCodec(new Lucene103Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return es819;
            }
        });
        try (Directory directory = new NIOFSDirectory(dir); IndexWriter writer = new IndexWriter(directory, config)) {
            for (int doc = 0; doc < docs; doc++) {
                Document document = new Document();
                if (hasUrl(doc)) document.add(new BinaryDocValuesField("url", new BytesRef(url(doc))));
                document.add(new NumericDocValuesField("counter", doc * 3L));
                document.add(new SortedSetDocValuesField("tags", new BytesRef("tag-" + (doc % 5))));
                document.add(new SortedSetDocValuesField("tags", new BytesRef("tag-" + (10 + doc % 3))));
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            writer.commit();
        }
    }

    /** Reads every value back and compares it exactly; returns {@code checked=<n> url=<format>/<type>}. */
    public static String read(Path dir, int docs) throws IOException {
        int checked = 0;
        String urlFormat = null;
        try (Directory directory = new NIOFSDirectory(dir); DirectoryReader reader = DirectoryReader.open(directory)) {
            if (reader.maxDoc() != docs) throw new AssertionError("maxDoc " + reader.maxDoc());
            for (LeafReaderContext leaf : reader.leaves()) {
                LeafReader leafReader = leaf.reader();
                FieldInfo url = leafReader.getFieldInfos().fieldInfo("url");
                if (url.getDocValuesType() != DocValuesType.BINARY) throw new AssertionError(url.getDocValuesType().toString());
                urlFormat = url.getAttribute("PerFieldDocValuesFormat.format") + "/" + url.getDocValuesType();
                BinaryDocValues urls = leafReader.getBinaryDocValues("url");
                NumericDocValues counters = leafReader.getNumericDocValues("counter");
                SortedSetDocValues tags = leafReader.getSortedSetDocValues("tags");
                for (int doc = 0; doc < leafReader.maxDoc(); doc++) {
                    int global = leaf.docBase + doc;
                    boolean has = urls.advanceExact(doc);
                    if (has != hasUrl(global)) throw new AssertionError("url presence at " + global);
                    if (has) {
                        if (!urls.binaryValue().utf8ToString().equals(url(global))) throw new AssertionError("url at " + global);
                        checked++;
                    }
                    if (!counters.advanceExact(doc) || counters.longValue() != global * 3L) throw new AssertionError("counter at " + global);
                    if (!tags.advanceExact(doc)) throw new AssertionError("tags at " + global);
                    Set<String> values = new TreeSet<>();
                    for (int i = 0; i < tags.docValueCount(); i++) values.add(tags.lookupOrd(tags.nextOrd()).utf8ToString());
                    if (!values.equals(new TreeSet<>(List.of("tag-" + (global % 5), "tag-" + (10 + global % 3))))) {
                        throw new AssertionError("tags at " + global + ": " + values);
                    }
                }
            }
        }
        return "checked=" + checked + " url=" + urlFormat;
    }
}
