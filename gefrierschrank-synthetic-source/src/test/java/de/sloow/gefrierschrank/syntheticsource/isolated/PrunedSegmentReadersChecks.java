package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Lucene-level checks of PrunedSegmentReadersTest, defined by the isolated loader: in the
 * surefire JVM itself Lucene's vector-format SPI cannot load Elasticsearch's providers. JUnit's
 * assertions are shared with the ambient world; arguments are JDK types.
 */
public final class PrunedSegmentReadersChecks {

    private static final int DOCS_PER_SEGMENT = 20;

    private PrunedSegmentReadersChecks() {}

    /** Two non-compound segments holding every structure, one hard delete. */
    static Path writeIndex(Path dir, boolean compoundThird) throws IOException {
        IsolatedEsBootstrap.ensure();
        try (Directory directory = new NIOFSDirectory(dir);
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setUseCompoundFile(false)
                 .setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < 2; segment++) {
                for (int i = 0; i < DOCS_PER_SEGMENT; i++) writer.addDocument(document(segment * DOCS_PER_SEGMENT + i));
                writer.commit();
            }
            writer.deleteDocuments(new Term("id", "3"));
            writer.commit();
        }
        if (compoundThird) {
            try (Directory directory = new NIOFSDirectory(dir);
                 IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setUseCompoundFile(true)
                     .setMergePolicy(NoMergePolicy.INSTANCE).setOpenMode(IndexWriterConfig.OpenMode.APPEND))) {
                for (int i = 0; i < DOCS_PER_SEGMENT; i++) writer.addDocument(document(2 * DOCS_PER_SEGMENT + i));
                writer.commit();
            }
        }
        return dir;
    }

    private static Document document(int id) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
        doc.add(new StoredField("stored", "value " + id));
        doc.add(new NumericDocValuesField("dv", id));
        doc.add(new TextField("text", "words of document " + id, Field.Store.NO));
        doc.add(new IntPoint("n", id));
        FieldType vectors = new FieldType(StringField.TYPE_NOT_STORED);
        vectors.setStoreTermVectors(true);
        doc.add(new Field("tv", "t" + id, vectors));
        doc.add(new KnnFloatVectorField("v", new float[] {id, 1, 2, 3}));
        return doc;
    }

    /** Deletes the files of {@code dir} that {@code drop} accepts; returns their names. */
    static List<String> delete(Path dir, Predicate<String> drop) throws IOException {
        List<String> deleted = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (drop.test(name)) {
                    Files.delete(file);
                    deleted.add(name);
                }
            }
        }
        return deleted;
    }

    static Predicate<String> of(Structure structure) {
        return name -> SelectiveFetch.structureOf(name) == structure;
    }

    /** Reads {@code structure} on every leaf; non-null when some leaf answered with values. */
    static Object access(LeafReader leaf, Structure structure) throws IOException {
        return switch (structure) {
            case POSTINGS -> leaf.terms("text");
            case POINTS -> leaf.getPointValues("n");
            case NORMS -> leaf.getNormValues("text");
            case TERM_VECTORS -> leaf.termVectors().get(0);
            case VECTORS -> leaf.getFloatVectorValues("v");
        };
    }

    public static void aGuardedAbsentStructureThrowsAndTrips(String structureName, String tmpDir) throws Exception {
        Structure structure = Structure.valueOf(structureName);
        Path tmp = Path.of(tmpDir);
        Path dir = writeIndex(tmp.resolve("index"), false);
        assertFalse(delete(dir, of(structure)).isEmpty(), "the index has files of " + structure);
        try (Directory directory = new NIOFSDirectory(dir)) {
            PrunedSegmentReaders.Opened opened = PrunedSegmentReaders.open(directory, PrunedSegmentReaders.Mode.GUARD);
            try (DirectoryReader reader = opened.reader()) {
                assertTrue(opened.pruned());
                assertDoesNotThrow(opened.tripwire()::check);
                for (Structure other : Structure.values()) {
                    if (other != structure) assertNotNull(access(reader.leaves().get(0).reader(), other), other.name());
                }
                assertFalse(opened.tripwire().tripped(), "present structures are the codec's own");
                assertThrows(PrunedFormatAccessException.class, () -> access(reader.leaves().get(0).reader(), structure));
                assertTrue(opened.tripwire().tripped());
                assertEquals(Set.of(structure), opened.tripwire().touched());
                assertThrows(PrunedFormatAccessException.class, opened.tripwire()::check);
            }
        }
    }

    public static void aProbedAbsentStructureAnswersNoValuesAndIsRecorded(String structureName, String tmpDir) throws Exception {
        Structure structure = Structure.valueOf(structureName);
        Path tmp = Path.of(tmpDir);
        Path dir = writeIndex(tmp.resolve("index"), false);
        delete(dir, of(structure));
        try (Directory directory = new NIOFSDirectory(dir)) {
            PrunedSegmentReaders.Opened opened = PrunedSegmentReaders.open(directory, PrunedSegmentReaders.Mode.PROBE);
            try (DirectoryReader reader = opened.reader()) {
                assertEquals(Set.of(), opened.tripwire().touched());
                for (LeafReaderContext leaf : reader.leaves()) assertNull(access(leaf.reader(), structure));
                assertEquals(Set.of(structure), opened.tripwire().touched());
                assertFalse(opened.tripwire().tripped());
                assertDoesNotThrow(opened.tripwire()::check);
            }
        }
    }

    public static void storedFieldsDocValuesAndDeletionsReadAsWithEveryFile(String tmpDir) throws Exception {
        Path tmp = Path.of(tmpDir);
        Path full = writeIndex(tmp.resolve("full"), true);
        Path pruned = Files.createDirectories(tmp.resolve("pruned"));
        try (Stream<Path> files = Files.list(full)) {
            for (Path file : files.toList()) Files.copy(file, pruned.resolve(file.getFileName()));
        }
        delete(pruned, name -> SelectiveFetch.structureOf(name) != null);
        try (Directory fullDirectory = new NIOFSDirectory(full); DirectoryReader expected = DirectoryReader.open(fullDirectory);
             Directory directory = new NIOFSDirectory(pruned)) {
            PrunedSegmentReaders.Opened opened = PrunedSegmentReaders.open(directory, PrunedSegmentReaders.Mode.GUARD);
            try (DirectoryReader reader = opened.reader()) {
                assertTrue(opened.pruned());
                assertEquals(layout(expected), layout(reader));
                for (int leaf = 0; leaf < reader.leaves().size(); leaf++) {
                    LeafReader actual = reader.leaves().get(leaf).reader();
                    LeafReader want = expected.leaves().get(leaf).reader();
                    for (int doc = 0; doc < actual.maxDoc(); doc++) {
                        assertEquals(want.storedFields().document(doc).get("stored"), actual.storedFields().document(doc).get("stored"));
                        assertEquals(want.getLiveDocs() == null || want.getLiveDocs().get(doc),
                            actual.getLiveDocs() == null || actual.getLiveDocs().get(doc));
                    }
                    assertEquals(values(want.getNumericDocValues("dv")), values(actual.getNumericDocValues("dv")));
                }
                // The compound segment (the third) keeps its structures inside the compound file.
                assertNotNull(reader.leaves().get(2).reader().terms("text"));
                assertFalse(opened.tripwire().tripped());
            }
        }
    }

    public static void withNothingAbsentTheOpenIsDirectoryReaderOpen(String tmpDir) throws Exception {
        Path tmp = Path.of(tmpDir);
        Path dir = writeIndex(tmp.resolve("index"), true);
        try (Directory directory = new NIOFSDirectory(dir); DirectoryReader expected = DirectoryReader.open(directory)) {
            for (PrunedSegmentReaders.Mode mode : PrunedSegmentReaders.Mode.values()) {
                PrunedSegmentReaders.Opened opened = PrunedSegmentReaders.open(directory, mode);
                try (DirectoryReader reader = opened.reader()) {
                    assertFalse(opened.pruned());
                    assertEquals(layout(expected), layout(reader));
                    for (LeafReaderContext leaf : reader.leaves()) {
                        Codec codec = ((SegmentReader) leaf.reader()).getSegmentInfo().info.getCodec();
                        assertFalse(codec instanceof PrunedSegmentReaders.PrunedCodec, "the segment's own codec");
                    }
                }
            }
        }
    }

    public static void thePrunedCodecHandsOutTheDelegatesOwnFormats(String tmpDir) throws Exception {
        Path tmp = Path.of(tmpDir);
        Path dir = writeIndex(tmp.resolve("index"), true);
        delete(dir, of(Structure.POSTINGS).or(of(Structure.VECTORS)));
        try (Directory directory = new NIOFSDirectory(dir)) {
            PrunedSegmentReaders.Opened opened = PrunedSegmentReaders.open(directory, PrunedSegmentReaders.Mode.GUARD);
            try (DirectoryReader reader = opened.reader()) {
                for (int i = 0; i < 2; i++) {
                    Codec codec = ((SegmentReader) reader.leaves().get(i).reader()).getSegmentInfo().info.getCodec();
                    var pruned = assertInstanceOf(PrunedSegmentReaders.PrunedCodec.class, codec);
                    Codec delegate = pruned.delegate();
                    assertEquals(Set.of(Structure.POSTINGS, Structure.VECTORS), pruned.absent());
                    assertSame(delegate.storedFieldsFormat(), pruned.storedFieldsFormat());
                    assertSame(delegate.compoundFormat(), pruned.compoundFormat());
                    assertSame(delegate.docValuesFormat(), pruned.docValuesFormat());
                    assertSame(delegate.fieldInfosFormat(), pruned.fieldInfosFormat());
                    assertSame(delegate.liveDocsFormat(), pruned.liveDocsFormat());
                    assertSame(delegate.segmentInfoFormat(), pruned.segmentInfoFormat());
                    // Lucene's codec builds these per call, so the delegation shows in the class.
                    assertEquals(delegate.pointsFormat().getClass(), pruned.pointsFormat().getClass());
                    assertEquals(delegate.normsFormat().getClass(), pruned.normsFormat().getClass());
                    assertEquals(delegate.termVectorsFormat().getClass(), pruned.termVectorsFormat().getClass());
                    assertEquals(delegate.getName(), pruned.getName());
                }
                Codec compound = ((SegmentReader) reader.leaves().get(2).reader()).getSegmentInfo().info.getCodec();
                assertFalse(compound instanceof PrunedSegmentReaders.PrunedCodec, "a compound segment is never pruned");
            }
        }
    }

    /** Files every reader opens: missing, they fail the open as without selective fetch, also beside an absent structure. */
    public static void aMissingFileOfNoStructureFailsTheOpenLikeANormalOpen(String extension, String tmpDir) throws Exception {
        Path tmp = Path.of(tmpDir);
        Path dir = writeIndex(tmp.resolve("index"), false);
        assertFalse(delete(dir, name -> name.startsWith("_0") && name.endsWith("." + extension)).isEmpty(), extension);
        Exception normal;
        try (Directory directory = new NIOFSDirectory(dir)) {
            normal = assertThrows(IOException.class, () -> DirectoryReader.open(directory).close());
        }
        assertTrue(missingFile(normal), extension);
        delete(dir, of(Structure.POSTINGS));
        for (PrunedSegmentReaders.Mode mode : PrunedSegmentReaders.Mode.values()) {
            try (Directory directory = new NIOFSDirectory(dir)) {
                Exception pruned = assertThrows(IOException.class, () -> PrunedSegmentReaders.open(directory, mode).reader().close(),
                    extension + " " + mode);
                // Which missing file Lucene meets first decides between NoSuchFileException and a
                // CorruptIndexException naming it; either way the segment was not opened pruned.
                assertTrue(missingFile(pruned), extension + " " + mode + ": " + pruned);
            }
        }
    }

    /** Lucene reports a missing file as such or as a corrupt index caused or accompanied by it. */
    private static boolean missingFile(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof NoSuchFileException) return true;
            for (Throwable suppressed : t.getSuppressed()) if (missingFile(suppressed)) return true;
        }
        return false;
    }

    record Layout(int leaves, List<Integer> maxDocs, List<Integer> docBases, int numDocs) {}

    static Layout layout(DirectoryReader reader) {
        List<Integer> maxDocs = new ArrayList<>();
        List<Integer> docBases = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            maxDocs.add(leaf.reader().maxDoc());
            docBases.add(leaf.docBase);
        }
        return new Layout(reader.leaves().size(), maxDocs, docBases, reader.numDocs());
    }

    private static List<Long> values(NumericDocValues values) throws IOException {
        List<Long> list = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) list.add(doc * 1_000_000L + values.longValue());
        return list;
    }
}
