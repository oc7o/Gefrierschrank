package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test helper defined by the isolated loader: ES 9.3's vector formats only open there. Lists every
 * live (neither hard- nor soft-deleted) document with a stored {@code _source} as {@code {doc id, _id, stored bytes}}, plus the
 * reader's {@code maxDoc} as the first element.
 */
public final class StoredSourceProbe {

    private StoredSourceProbe() {}

    public static List<Object[]> liveRoots(Path shardDir) throws IOException {
        IsolatedEsBootstrap.ensure();
        List<Object[]> roots = new ArrayList<>();
        try (Directory directory = new NIOFSDirectory(shardDir); DirectoryReader reader =
                 new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(directory), "__soft_deletes")) {
            roots.add(new Object[] {reader.maxDoc()});
            var live = MultiBits.getLiveDocs(reader);
            StoredFields stored = reader.storedFields();
            for (int doc = 0; doc < reader.maxDoc(); doc++) {
                if (live != null && !live.get(doc)) continue;
                var document = stored.document(doc);
                BytesRef source = document.getBinaryValue("_source");
                if (source == null) continue;
                BytesRef id = document.getBinaryValue("_id");
                roots.add(new Object[] {doc, Uid.decodeId(id.bytes, id.offset, id.length),
                    Arrays.copyOfRange(source.bytes, source.offset, source.offset + source.length)});
            }
        }
        return roots;
    }
}
