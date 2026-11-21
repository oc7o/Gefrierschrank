package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Opens a shard directory from which selective fetch left out whole index structures (postings,
 * points, norms, term vectors, vectors) of non-compound segments. What is left out is derived from
 * the directory itself: a structure is absent from a segment when some file of that segment
 * belonging to it is not there. Such a segment is opened with its own codec, except that each
 * absent structure is answered by a stub that opens no file. With nothing absent the open is
 * exactly {@link DirectoryReader#open(Directory)}, so a fully fetched shard reads as before.
 *
 * <p>{@link Mode#PROBE} stubs answer "no values" and record the access, so the fetch probe learns
 * what Elasticsearch's loaders read. {@link Mode#GUARD} stubs record the access, trip the
 * {@link Tripwire} and throw {@link PrunedFormatAccessException}; the worker checks the tripwire
 * before returning any document, so even an access whose exception Elasticsearch code swallowed
 * fails the read instead of producing a document with missing values.
 */
final class PrunedSegmentReaders {

    enum Mode { PROBE, GUARD }

    /** {@code pruned}: some segment was opened with stubs. */
    record Opened(DirectoryReader reader, Tripwire tripwire, boolean pruned) {}

    private PrunedSegmentReaders() {}

    /**
     * Opens the latest commit of {@code directory}. A missing file that belongs to no structure
     * (stored fields, doc values, field infos, live docs, anything unknown) prunes nothing in its
     * segment, so the open fails with the {@code NoSuchFileException} a normal open gives.
     */
    static Opened open(Directory directory, Mode mode) throws IOException {
        Tripwire tripwire = new Tripwire();
        SegmentInfos infos = SegmentInfos.readLatestCommit(directory, Version.MIN_SUPPORTED_MAJOR);
        Set<String> present = Set.of(directory.listAll());
        SegmentInfos pruned = infos.clone();
        pruned.clear();
        boolean anyAbsent = false;
        for (SegmentCommitInfo info : infos) {
            Set<Structure> absent = absent(info, present);
            if (absent.isEmpty()) {
                pruned.add(info);
            } else {
                anyAbsent = true;
                pruned.add(withCodec(info, new PrunedCodec(info.info.getCodec(), absent, tripwire, mode)));
            }
        }
        if (!anyAbsent) return new Opened(DirectoryReader.open(directory), tripwire, false);
        // One SegmentReader per segment in commit order, as DirectoryReader.open builds them.
        return new Opened(StandardDirectoryReader.open(directory, pruned, null, null), tripwire, true);
    }

    /** The structures with a missing file in a non-compound segment; none when a non-structure file is missing too. */
    static Set<Structure> absent(SegmentCommitInfo info, Set<String> present) throws IOException {
        if (info.info.getUseCompoundFile()) return Set.of();
        Set<Structure> absent = EnumSet.noneOf(Structure.class);
        Set<String> files = new HashSet<>(info.files());
        for (String file : files) {
            if (present.contains(file)) continue;
            Structure structure = SelectiveFetch.structureOf(file);
            if (structure == null) return Set.of();
            absent.add(structure);
        }
        return absent;
    }

    /** A copy with the codec replaced: SegmentInfo.setCodec asserts that no codec is set yet. */
    static SegmentCommitInfo withCodec(SegmentCommitInfo info, Codec codec) {
        SegmentInfo si = info.info;
        var copy = new SegmentInfo(si.dir, si.getVersion(), si.getMinVersion(), si.name, si.maxDoc(),
            si.getUseCompoundFile(), si.getHasBlocks(), codec, si.getDiagnostics(), si.getId(), si.getAttributes(),
            si.getIndexSort());
        copy.setFiles(si.files());
        var commit = new SegmentCommitInfo(copy, info.getDelCount(), info.getSoftDelCount(), info.getDelGen(),
            info.getFieldInfosGen(), info.getDocValuesGen(), info.getId());
        commit.setFieldInfosFiles(info.getFieldInfosFiles());
        commit.setDocValuesUpdatesFiles(info.getDocValuesUpdatesFiles());
        return commit;
    }

    /**
     * What the stubs of one open reader saw: the structures they were asked for and, in GUARD mode,
     * whether any was. Thread-safe; shared by every segment of the reader.
     */
    static final class Tripwire {
        private final AtomicBoolean tripped = new AtomicBoolean();
        private final Set<Structure> touched = EnumSet.noneOf(Structure.class);

        void record(Structure structure) {
            synchronized (touched) {
                touched.add(structure);
            }
        }

        void trip() {
            tripped.set(true);
        }

        boolean tripped() {
            return tripped.get();
        }

        Set<Structure> touched() {
            synchronized (touched) {
                return touched.isEmpty() ? Set.of() : Collections.unmodifiableSet(EnumSet.copyOf(touched));
            }
        }

        /** Throws once any guard stub was read, whether or not its exception reached the caller. */
        void check() throws PrunedFormatAccessException {
            if (tripped.get()) throw new PrunedFormatAccessException();
        }
    }

    /**
     * The segment's own codec with the absent structures' formats replaced. Everything else is the
     * delegate's own object, stored fields and the compound format included: the ZSTD stored-fields
     * replacement ({@link JniZstdStoredFieldsReader}) recognises segments by that format's class.
     * Never registered with SPI and never written with.
     */
    static final class PrunedCodec extends FilterCodec {
        private final Set<Structure> absent;
        private final Tripwire tripwire;
        private final Mode mode;

        PrunedCodec(Codec delegate, Set<Structure> absent, Tripwire tripwire, Mode mode) {
            super(delegate.getName(), delegate);
            this.absent = Set.copyOf(absent);
            this.tripwire = tripwire;
            this.mode = mode;
        }

        Codec delegate() {
            return delegate;
        }

        Set<Structure> absent() {
            return absent;
        }

        /** Records the access; in GUARD mode trips and throws, in PROBE mode the caller answers "no values". */
        private void access(Structure structure) throws PrunedFormatAccessException {
            tripwire.record(structure);
            if (mode == Mode.GUARD) {
                tripwire.trip();
                throw new PrunedFormatAccessException();
            }
        }

        @Override
        public PostingsFormat postingsFormat() {
            return absent.contains(Structure.POSTINGS) ? postings() : delegate.postingsFormat();
        }

        @Override
        public PointsFormat pointsFormat() {
            return absent.contains(Structure.POINTS) ? points() : delegate.pointsFormat();
        }

        @Override
        public NormsFormat normsFormat() {
            return absent.contains(Structure.NORMS) ? norms() : delegate.normsFormat();
        }

        @Override
        public TermVectorsFormat termVectorsFormat() {
            return absent.contains(Structure.TERM_VECTORS) ? termVectors() : delegate.termVectorsFormat();
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return absent.contains(Structure.VECTORS) ? vectors() : delegate.knnVectorsFormat();
        }

        private PostingsFormat postings() {
            return new PostingsFormat("PrunedPostings") {
                @Override
                public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
                    throw readOnly();
                }

                @Override
                public FieldsProducer fieldsProducer(SegmentReadState state) {
                    return new FieldsProducer() {
                        @Override
                        public Iterator<String> iterator() {
                            try {
                                access(Structure.POSTINGS);
                            } catch (PrunedFormatAccessException e) {
                                throw new java.io.UncheckedIOException(e);
                            }
                            return Collections.emptyIterator();
                        }

                        @Override
                        public Terms terms(String field) throws PrunedFormatAccessException {
                            access(Structure.POSTINGS);
                            return null;
                        }

                        @Override
                        public int size() {
                            return -1;
                        }

                        @Override
                        public void checkIntegrity() {}

                        @Override
                        public void close() {}
                    };
                }
            };
        }

        private NormsFormat norms() {
            return new NormsFormat() {
                @Override
                public NormsConsumer normsConsumer(SegmentWriteState state) {
                    throw readOnly();
                }

                @Override
                public NormsProducer normsProducer(SegmentReadState state) {
                    return new NormsProducer() {
                        @Override
                        public NumericDocValues getNorms(FieldInfo field) throws PrunedFormatAccessException {
                            access(Structure.NORMS);
                            return null;
                        }

                        @Override
                        public void checkIntegrity() {}

                        @Override
                        public void close() {}
                    };
                }
            };
        }

        private PointsFormat points() {
            return new PointsFormat() {
                @Override
                public PointsWriter fieldsWriter(SegmentWriteState state) {
                    throw readOnly();
                }

                @Override
                public PointsReader fieldsReader(SegmentReadState state) {
                    return new PointsReader() {
                        @Override
                        public PointValues getValues(String field) throws PrunedFormatAccessException {
                            access(Structure.POINTS);
                            return null;
                        }

                        @Override
                        public void checkIntegrity() {}

                        @Override
                        public void close() {}
                    };
                }
            };
        }

        private TermVectorsFormat termVectors() {
            return new TermVectorsFormat() {
                @Override
                public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos,
                                                       IOContext context) {
                    return new StubTermVectorsReader();
                }

                @Override
                public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) {
                    throw readOnly();
                }
            };
        }

        /** {@code prefetch} stays the no-op default: it returns no data. */
        private final class StubTermVectorsReader extends TermVectorsReader {
            @Override
            public Fields get(int doc) throws PrunedFormatAccessException {
                access(Structure.TERM_VECTORS);
                return null;
            }

            @Override
            public TermVectorsReader clone() {
                return this;
            }

            @Override
            public void checkIntegrity() {}

            @Override
            public void close() {}
        }

        private KnnVectorsFormat vectors() {
            return new KnnVectorsFormat("PrunedKnnVectors") {
                @Override
                public KnnVectorsWriter fieldsWriter(SegmentWriteState state) {
                    throw readOnly();
                }

                @Override
                public KnnVectorsReader fieldsReader(SegmentReadState state) {
                    return new KnnVectorsReader() {
                        @Override
                        public FloatVectorValues getFloatVectorValues(String field) throws PrunedFormatAccessException {
                            access(Structure.VECTORS);
                            return null;
                        }

                        @Override
                        public ByteVectorValues getByteVectorValues(String field) throws PrunedFormatAccessException {
                            access(Structure.VECTORS);
                            return null;
                        }

                        @Override
                        public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
                                throws PrunedFormatAccessException {
                            access(Structure.VECTORS);
                        }

                        @Override
                        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
                                throws PrunedFormatAccessException {
                            access(Structure.VECTORS);
                        }

                        @Override
                        public void checkIntegrity() {}

                        @Override
                        public void close() {}
                    };
                }

                @Override
                public int getMaxDimensions(String fieldName) {
                    return DEFAULT_MAX_DIMENSIONS;
                }
            };
        }

        private static UnsupportedOperationException readOnly() {
            return new UnsupportedOperationException("read-only format");
        }
    }
}
