package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene102.Lucene102HnswBinaryQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The pruned open: extensions pinned to the Lucene 10.3.2 formats, identical documents on an
 * Elasticsearch-shaped multi-segment index with every pruned structure present, and no pruned
 * file opened.
 */
class PrunedCodecTest {

    static final String SOFT_DELETES = "__soft_deletes";

    // ---------------------------------------------------------------- extension pinning

    /** Lucene 10.3.2's codec with the given kNN format for every vector field. */
    static Codec codecWith(KnnVectorsFormat vectors) {
        return new Lucene103Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return vectors;
            }
        };
    }

    static Set<String> extensionsWritten(Path dir, Codec codec, boolean compound, Consumer<Document> structure,
                                         boolean deleteOne) throws IOException {
        try (var directory = new NIOFSDirectory(dir);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setCodec(codec).setUseCompoundFile(compound)
                 .setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < 2; i++) {
                var doc = new Document();
                doc.add(new StoredField("_source", new BytesRef("{}")));
                doc.add(new StringField("id", "doc" + i, Field.Store.NO));
                structure.accept(doc);
                writer.addDocument(doc);
            }
            if (deleteOne) writer.deleteDocuments(new Term("id", "doc0"));
            writer.commit();
        }
        try (var files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                .filter(name -> !name.startsWith("segments_") && !name.equals("write.lock"))
                .map(SelectiveFetch::extension).collect(Collectors.toCollection(TreeSet::new));
        }
    }

    @Test
    void everyFileOfAPrunedFormatHasAPrunedExtensionAndNoKeptFormatDoes(@TempDir Path tmp) throws IOException {
        Codec codec = new Lucene103Codec();
        // The id field's postings are part of every index; the kept baseline is measured without it.
        Set<String> base = extensionsWritten(tmp.resolve("base"), codec, false, doc -> {}, false);
        Set<String> postings = Set.of("doc", "tim", "tip", "tmd", "psm");
        Set<String> kept = new TreeSet<>(base);
        kept.removeAll(postings);
        assertEquals(Set.of("fdm", "fdt", "fdx", "fnm", "si"), kept, "stored fields, field infos, segment info");
        assertTrue(base.containsAll(postings), "the id field writes the postings files: " + base);

        Map<String, Consumer<Document>> structures = Map.of(
            "positions and payloads", doc -> {
                var type = new FieldType(TextField.TYPE_NOT_STORED);
                type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                doc.add(new Field("text", "some words here", type));
            },
            "points", doc -> doc.add(new IntPoint("point", 7)),
            "norms", doc -> doc.add(new TextField("text", "some words", Field.Store.NO)),
            "term vectors", doc -> {
                var type = new FieldType(StringField.TYPE_NOT_STORED);
                type.setStoreTermVectors(true);
                type.setStoreTermVectorPositions(true);
                type.setStoreTermVectorOffsets(true);
                doc.add(new Field("tv", "term", type));
            },
            "float vectors", doc -> doc.add(new KnnFloatVectorField("vector", new float[] {1, 2, 3, 4})));
        for (var structure : structures.entrySet()) {
            Set<String> written = extensionsWritten(tmp.resolve("s-" + structure.getKey().replace(' ', '-')), codec, false,
                structure.getValue(), false);
            written.removeAll(base);
            assertFalse(written.isEmpty(), structure.getKey());
            assertTrue(SelectiveFetch.PRUNED_EXTENSIONS.containsAll(written), structure.getKey() + ": " + written);
        }
        Map<String, KnnVectorsFormat> vectorFormats = Map.of(
            "hnsw", new Lucene99HnswVectorsFormat(),
            "scalar", new Lucene99HnswScalarQuantizedVectorsFormat(),
            "binary", new Lucene102HnswBinaryQuantizedVectorsFormat());
        for (var format : vectorFormats.entrySet()) {
            Set<String> written = extensionsWritten(tmp.resolve("v-" + format.getKey()), codecWith(format.getValue()), false,
                doc -> doc.add(new KnnFloatVectorField("vector", new float[] {1, 2, 3, 4})), false);
            written.removeAll(base);
            assertFalse(written.isEmpty(), format.getKey());
            assertTrue(SelectiveFetch.PRUNED_EXTENSIONS.containsAll(written), format.getKey() + ": " + written);
        }

        Set<String> docValues = extensionsWritten(tmp.resolve("dv"), codec, false,
            doc -> doc.add(new NumericDocValuesField("_primary_term", 1)), true);
        docValues.removeAll(base);
        assertEquals(Set.of("dvd", "dvm", "liv"), docValues, "doc values and live docs");
        Set<String> compound = extensionsWritten(tmp.resolve("cfs"), codec, true, doc -> {}, false);
        assertEquals(Set.of("cfe", "cfs", "si"), compound);
        for (String extension : List.of("fdm", "fdt", "fdx", "fnm", "si", "dvd", "dvm", "liv", "cfs", "cfe"))
            assertFalse(SelectiveFetch.PRUNED_EXTENSIONS.contains(extension), extension);
    }

    // ---------------------------------------------------------------- an Elasticsearch-shaped index

    static Document root(int id, boolean withStructures) {
        var doc = new Document();
        doc.add(new StoredField("_id", new BytesRef(("id-" + id).getBytes(StandardCharsets.UTF_8))));
        doc.add(new StringField("id", "id-" + id, Field.Store.NO));
        if (id % 3 == 0) doc.add(new StoredField("_routing", "r" + id));
        doc.add(new StoredField("_source", new BytesRef(("{\"n\":" + id + ",\"text\":\"words " + id + "\"}").getBytes(StandardCharsets.UTF_8))));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        doc.add(new SortedDocValuesField("keyword", new BytesRef("k" + id % 5)));
        if (withStructures) {
            doc.add(new TextField("text", "words " + id, Field.Store.NO));
            doc.add(new IntPoint("point", id));
            var type = new FieldType(StringField.TYPE_NOT_STORED);
            type.setStoreTermVectors(true);
            doc.add(new Field("tv", "t" + id, type));
            doc.add(new KnnFloatVectorField("vector", new float[] {id, 1, 2, 3}));
        }
        return doc;
    }

    static Document child(int id) {
        var doc = new Document();
        doc.add(new StringField("id", "id-" + id, Field.Store.NO));
        doc.add(new StringField("_nested_path", "children", Field.Store.NO));
        doc.add(new IntPoint("point", -id));
        return doc;
    }

    /** Four segments: plain roots, nested blocks, soft updates (a doc-values update generation), hard deletes. */
    static void writeEsShapedIndex(Path dir, boolean compound) throws IOException {
        try (var directory = new NIOFSDirectory(dir);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setSoftDeletesField(SOFT_DELETES)
                 .setUseCompoundFile(compound).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int id = 0; id < 30; id++) writer.addDocument(root(id, id % 2 == 0));
            writer.commit();
            for (int id = 30; id < 40; id++) writer.addDocuments(List.of(child(id), child(id), root(id, true)));
            writer.commit();
            for (int id = 0; id < 30; id += 4) {
                writer.softUpdateDocument(new Term("id", "id-" + id), root(id + 100, true),
                    new NumericDocValuesField(SOFT_DELETES, 1));
            }
            writer.commit();
            writer.deleteDocuments(new Term("id", "id-1"), new Term("id", "id-31"), new Term("id", "id-104"));
            for (int id = 200; id < 210; id++) writer.addDocument(root(id, false));
            writer.commit();
        }
    }

    /** Every observable of the reader per document, exceptions as class and message. */
    static List<String> observe(ShardReader reader) throws IOException {
        var seen = new ArrayList<String>();
        seen.add("docs=" + reader.docCount());
        for (int doc = 0; doc < reader.docCount(); doc++) {
            String metadata;
            try {
                metadata = reader.documentMetadata(doc).toString();
            } catch (IOException | RuntimeException e) {
                metadata = e.getClass().getName() + ": " + e.getMessage();
            }
            var source = new String[1];
            try {
                reader.readSource(doc, (bytes, length) -> source[0] = new String(bytes, 0, length, StandardCharsets.UTF_8));
            } catch (IOException | RuntimeException e) {
                source[0] = e.getClass().getName() + ": " + e.getMessage();
            }
            seen.add(doc + " live=" + reader.isLive(doc) + " absent=" + reader.storedSourceAbsent(doc) + " " + metadata + " " + source[0]);
        }
        return seen;
    }

    static List<String> leafLayout(DirectoryReader reader) {
        return reader.leaves().stream().map(l -> l.ord + ":" + l.docBase + "+" + l.reader().maxDoc()).toList();
    }

    @Test
    void thePrunedOpenReadsEveryDocumentLikeTheFullOpen(@TempDir Path tmp) throws Exception {
        for (boolean compound : new boolean[] {false, true}) {
            Path dir = tmp.resolve("index-" + compound);
            writeEsShapedIndex(dir, compound);
            List<String> expected;
            try (var full = new Lucene10ShardReader(dir)) {
                expected = observe(full);
            }
            try (var pruned = Lucene10ShardReader.openPruned(dir)) {
                assertEquals(expected, observe(pruned), "compound=" + compound);
                var fork = pruned.fork();
                assertEquals(expected, observe(fork));
                fork.close();
            }
            try (var directory = new NIOFSDirectory(dir);
                 var full = DirectoryReader.open(directory);
                 var pruned = Lucene10ShardReader.openPrunedReader(directory)) {
                assertEquals(leafLayout(full), leafLayout(pruned));
                assertEquals(4, pruned.leaves().size());
            }
        }
    }

    @Test
    void thePrunedOpenNeedsNoPrunedFile(@TempDir Path tmp) throws Exception {
        Path dir = tmp.resolve("index");
        writeEsShapedIndex(dir, false);
        List<String> expected;
        try (var full = new Lucene10ShardReader(dir)) {
            expected = observe(full);
        }
        var deleted = new TreeSet<String>();
        try (var files = Files.list(dir)) {
            for (Path file : files.toList()) {
                String extension = SelectiveFetch.extension(file.getFileName().toString());
                if (extension != null && SelectiveFetch.PRUNED_EXTENSIONS.contains(extension)) {
                    Files.delete(file);
                    deleted.add(extension);
                }
            }
        }
        assertTrue(deleted.containsAll(Set.of("doc", "tim", "kdd", "nvd", "tvd", "vec")), "every structure was present: " + deleted);
        try (var pruned = Lucene10ShardReader.openPruned(dir)) {
            assertEquals(expected, observe(pruned));
        }
        assertThrows(IOException.class, () -> new Lucene10ShardReader(dir).close(), "the full open needs them");
    }

    /** Records every file a reader opens. */
    static final class RecordingDirectory extends FilterDirectory {
        final Set<String> opened = ConcurrentHashMap.newKeySet();

        RecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            opened.add(name);
            return super.openInput(name, context);
        }

        @Override
        public ChecksumIndexInput openChecksumInput(String name) throws IOException {
            opened.add(name);
            return super.openChecksumInput(name);
        }
    }

    @Test
    void thePrunedReaderOpensOnlyKeptFiles(@TempDir Path tmp) throws Exception {
        Path dir = tmp.resolve("index");
        writeEsShapedIndex(dir, false);
        var recording = new RecordingDirectory(new NIOFSDirectory(dir));
        try (var pruned = Lucene10ShardReader.openPruned(recording)) {
            observe(pruned);
        }
        Set<String> extensions = recording.opened.stream().map(SelectiveFetch::extension)
            .filter(e -> e != null).collect(Collectors.toCollection(TreeSet::new));
        assertTrue(extensions.containsAll(Set.of("si", "fnm", "fdt", "dvd", "liv")), extensions.toString());
        for (String extension : extensions)
            assertFalse(SelectiveFetch.PRUNED_EXTENSIONS.contains(extension), "opened " + extension);
    }

    @Test
    void readingAPrunedStructureThrows(@TempDir Path tmp) throws Exception {
        Path dir = tmp.resolve("index");
        writeEsShapedIndex(dir, false);
        try (var directory = new NIOFSDirectory(dir); var reader = Lucene10ShardReader.openPrunedReader(directory)) {
            LeafReader leaf = reader.leaves().get(1).reader();
            assertThrows(PrunedFormatAccessException.class, () -> leaf.terms("text"));
            assertThrows(PrunedFormatAccessException.class, () -> leaf.postings(new Term("id", "id-30")));
            assertThrows(PrunedFormatAccessException.class, () -> leaf.getPointValues("point"));
            assertThrows(PrunedFormatAccessException.class, () -> leaf.getNormValues("text"));
            assertThrows(PrunedFormatAccessException.class, () -> leaf.termVectors().get(2));
            assertThrows(PrunedFormatAccessException.class, () -> leaf.getFloatVectorValues("vector"));
            var codec = (org.apache.lucene.index.CodecReader) leaf;
            var iteration = assertThrows(UncheckedIOException.class, () -> codec.getPostingsReader().iterator());
            assertInstanceOf(PrunedFormatAccessException.class, iteration.getCause());
            assertEquals("a pruned index structure was read (selective fetch)", new PrunedFormatAccessException().getMessage());
            assertTrue(leaf.getNumericDocValues("_primary_term") != null, "doc values are the delegate's");
        }
    }

    @Test
    void legacyNestedMarkersFailThePrunedOpenLoudly(@TempDir Path tmp) throws Exception {
        try (var directory = new NIOFSDirectory(tmp);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setUseCompoundFile(false))) {
            var root = new Document();
            root.add(new StoredField("_id", new BytesRef("1")));
            root.add(new StoredField("_source", new BytesRef("{}")));
            writer.addDocuments(List.of(child(1), root));
        }
        assertThrows(PrunedFormatAccessException.class, () -> Lucene10ShardReader.openPruned(tmp));
        try (var full = new Lucene10ShardReader(tmp)) {
            assertFalse(full.isLive(0));
        }
    }

    @Test
    void thePrunedCodecDelegatesEverythingTheExportReads() {
        Codec delegate = new Lucene103Codec();
        var pruned = new PrunedCodec(delegate);
        assertEquals(delegate.getName(), pruned.getName());
        assertSame(delegate, pruned.delegate());
        assertSame(delegate.storedFieldsFormat(), pruned.storedFieldsFormat());
        assertSame(delegate.fieldInfosFormat(), pruned.fieldInfosFormat());
        assertSame(delegate.segmentInfoFormat(), pruned.segmentInfoFormat());
        assertSame(delegate.liveDocsFormat(), pruned.liveDocsFormat());
        assertSame(delegate.compoundFormat(), pruned.compoundFormat());
        assertSame(delegate.docValuesFormat(), pruned.docValuesFormat());
        assertSame(PrunedFormats.POSTINGS, pruned.postingsFormat());
        assertSame(PrunedFormats.POINTS, pruned.pointsFormat());
        assertSame(PrunedFormats.NORMS, pruned.normsFormat());
        assertSame(PrunedFormats.TERM_VECTORS, pruned.termVectorsFormat());
        assertSame(PrunedFormats.KNN_VECTORS, pruned.knnVectorsFormat());
    }
}
