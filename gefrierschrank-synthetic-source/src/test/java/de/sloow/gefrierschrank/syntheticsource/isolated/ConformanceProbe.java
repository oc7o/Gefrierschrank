package de.sloow.gefrierschrank.syntheticsource.isolated;

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
import org.apache.lucene.index.LeafReaderContext;
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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test helper of SyntheticFetchConformanceTest, defined by the isolated loader: what a complete
 * reconstruction of every live root of a shard reads. The shard is opened with every segment's
 * codec wrapped by recording formats around its real producers and through a directory that
 * records the files opened once the reader is open; the worker's setup, leaf entry and both
 * loader paths then run over every live root. JDK types only across the boundary.
 */
public final class ConformanceProbe {

    private ConformanceProbe() {}

    /** The Elasticsearch version of the isolated world's classes. */
    public static String esVersion() {
        return org.elasticsearch.Version.CURRENT.toString();
    }

    /** Global ids of the documents the export reads (live roots), ascending. */
    public static int[] liveRoots(String shardDir) throws IOException {
        IsolatedEsBootstrap.ensure();
        List<Integer> roots = new ArrayList<>();
        try (Directory directory = new NIOFSDirectory(Path.of(shardDir)); DirectoryReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                for (int doc : IsolatedSyntheticFetchProbeImpl.liveRoots(leaf.reader())) roots.add(leaf.docBase + doc);
            }
        }
        return roots.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Reconstructs every live root of the complete shard in {@code shardDir} and returns
     * {@code "structure:<name>"} for every structure of a non-compound segment that was read and
     * {@code "file:<name>"} for every file opened after the reader was open.
     */
    public static List<String> reads(String shardDir, String indexName, String mappingJson, Map<String, String> flatSettings,
                                     String rewriteMappingJson) throws IOException {
        Set<Structure> structures = Collections.synchronizedSet(EnumSet.noneOf(Structure.class));
        Set<String> opened = Collections.synchronizedSet(new TreeSet<>());
        AtomicBoolean recordingOpens = new AtomicBoolean();
        try (IsolatedReconstructionWorkerImpl.Setup setup =
                 IsolatedReconstructionWorkerImpl.Setup.build(indexName, mappingJson, flatSettings, rewriteMappingJson);
             Directory directory = new FilterDirectory(new NIOFSDirectory(Path.of(shardDir))) {
                 @Override
                 public IndexInput openInput(String name, IOContext context) throws IOException {
                     if (recordingOpens.get()) opened.add(name);
                     return super.openInput(name, context);
                 }
             }) {
            SegmentInfos infos = SegmentInfos.readLatestCommit(directory, Version.MIN_SUPPORTED_MAJOR);
            SegmentInfos recording = infos.clone();
            recording.clear();
            for (SegmentCommitInfo info : infos) {
                recording.add(PrunedSegmentReaders.withCodec(info,
                    new RecordingCodec(info.info.getCodec(), info.info.getUseCompoundFile() ? null : structures)));
            }
            try (DirectoryReader reader = SequentialStoredFieldsDirectoryReader.wrap(JniZstdStoredFieldsReader.wrap(
                     StandardDirectoryReader.open(directory, recording, null, null)))) {
                recordingOpens.set(true);
                for (LeafReaderContext leaf : reader.leaves()) {
                    int[] live = IsolatedSyntheticFetchProbeImpl.liveRoots(leaf.reader());
                    if (live.length == 0) continue;
                    int[] roots = IsolatedSyntheticFetchProbeImpl.roots(setup.enterLeaf(leaf), live);
                    if (roots.length == 0) continue;
                    IsolatedSyntheticFetchProbeImpl.reconstruct(setup, leaf, roots, null);
                    IsolatedSyntheticFetchProbeImpl.reconstruct(setup, leaf, roots, roots);
                }
            }
        }
        List<String> result = new ArrayList<>();
        for (Structure structure : new TreeSet<>(structures)) result.add("structure:" + structure.name());
        for (String name : opened) result.add("file:" + name);
        return result;
    }

    /** The real codec, its five optional formats' readers wrapped to record data reads; {@code read} null records nothing. */
    private static final class RecordingCodec extends FilterCodec {
        private final Set<Structure> read;

        RecordingCodec(Codec delegate, Set<Structure> read) {
            super(delegate.getName(), delegate);
            this.read = read;
        }

        private void record(Structure structure) {
            if (read != null) read.add(structure);
        }

        @Override
        public PostingsFormat postingsFormat() {
            PostingsFormat real = delegate.postingsFormat();
            return new PostingsFormat(real.getName()) {
                @Override
                public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
                    FieldsProducer producer = real.fieldsProducer(state);
                    return new FieldsProducer() {
                        @Override
                        public Iterator<String> iterator() {
                            record(Structure.POSTINGS);
                            return producer.iterator();
                        }

                        @Override
                        public Terms terms(String field) throws IOException {
                            record(Structure.POSTINGS);
                            return producer.terms(field);
                        }

                        @Override
                        public int size() {
                            return producer.size();
                        }

                        @Override
                        public void checkIntegrity() throws IOException {
                            producer.checkIntegrity();
                        }

                        @Override
                        public void close() throws IOException {
                            producer.close();
                        }
                    };
                }
            };
        }

        @Override
        public NormsFormat normsFormat() {
            NormsFormat real = delegate.normsFormat();
            return new NormsFormat() {
                @Override
                public NormsConsumer normsConsumer(SegmentWriteState state) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public NormsProducer normsProducer(SegmentReadState state) throws IOException {
                    NormsProducer producer = real.normsProducer(state);
                    return new NormsProducer() {
                        @Override
                        public NumericDocValues getNorms(FieldInfo field) throws IOException {
                            record(Structure.NORMS);
                            return producer.getNorms(field);
                        }

                        @Override
                        public void checkIntegrity() throws IOException {
                            producer.checkIntegrity();
                        }

                        @Override
                        public void close() throws IOException {
                            producer.close();
                        }
                    };
                }
            };
        }

        @Override
        public PointsFormat pointsFormat() {
            PointsFormat real = delegate.pointsFormat();
            return new PointsFormat() {
                @Override
                public PointsWriter fieldsWriter(SegmentWriteState state) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public PointsReader fieldsReader(SegmentReadState state) throws IOException {
                    PointsReader reader = real.fieldsReader(state);
                    return new PointsReader() {
                        @Override
                        public PointValues getValues(String field) throws IOException {
                            record(Structure.POINTS);
                            return reader.getValues(field);
                        }

                        @Override
                        public void checkIntegrity() throws IOException {
                            reader.checkIntegrity();
                        }

                        @Override
                        public void close() throws IOException {
                            reader.close();
                        }
                    };
                }
            };
        }

        @Override
        public TermVectorsFormat termVectorsFormat() {
            TermVectorsFormat real = delegate.termVectorsFormat();
            return new TermVectorsFormat() {
                @Override
                public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos,
                                                       IOContext context) throws IOException {
                    return new RecordingTermVectors(real.vectorsReader(directory, segmentInfo, fieldInfos, context));
                }

                @Override
                public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private final class RecordingTermVectors extends TermVectorsReader {
            private final TermVectorsReader reader;

            RecordingTermVectors(TermVectorsReader reader) {
                this.reader = reader;
            }

            @Override
            public Fields get(int doc) throws IOException {
                record(Structure.TERM_VECTORS);
                return reader.get(doc);
            }

            @Override
            public TermVectorsReader clone() {
                return new RecordingTermVectors(reader.clone());
            }

            @Override
            public void checkIntegrity() throws IOException {
                reader.checkIntegrity();
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            KnnVectorsFormat real = delegate.knnVectorsFormat();
            return new KnnVectorsFormat(real.getName()) {
                @Override
                public KnnVectorsWriter fieldsWriter(SegmentWriteState state) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                    KnnVectorsReader reader = real.fieldsReader(state);
                    return new KnnVectorsReader() {
                        @Override
                        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
                            record(Structure.VECTORS);
                            return reader.getFloatVectorValues(field);
                        }

                        @Override
                        public ByteVectorValues getByteVectorValues(String field) throws IOException {
                            record(Structure.VECTORS);
                            return reader.getByteVectorValues(field);
                        }

                        @Override
                        public void search(String field, float[] target, KnnCollector collector, AcceptDocs acceptDocs)
                                throws IOException {
                            record(Structure.VECTORS);
                            reader.search(field, target, collector, acceptDocs);
                        }

                        @Override
                        public void search(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs)
                                throws IOException {
                            record(Structure.VECTORS);
                            reader.search(field, target, collector, acceptDocs);
                        }

                        @Override
                        public void checkIntegrity() throws IOException {
                            reader.checkIntegrity();
                        }

                        @Override
                        public void close() throws IOException {
                            reader.close();
                        }
                    };
                }

                @Override
                public int getMaxDimensions(String fieldName) {
                    return real.getMaxDimensions(fieldName);
                }
            };
        }
    }
}
