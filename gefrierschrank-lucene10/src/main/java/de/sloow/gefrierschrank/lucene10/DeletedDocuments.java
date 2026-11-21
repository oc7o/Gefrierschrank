package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import java.io.IOException;
import java.util.BitSet;

/** Snapshot readers must exclude both hard-deleted and retained soft-deleted versions. */
final class DeletedDocuments {
    static BitSet load(DirectoryReader reader) throws IOException {
        BitSet deleted = new BitSet(reader.maxDoc());
        for (var leaf : reader.leaves()) {
            // ES >= 6.1 identifies roots via _primary_term doc values (Queries.newNonNestedFilter).
            // This also works with our stored-source readers' intentionally stubbed postings.
            boolean hasRootMarker = leaf.reader().getFieldInfos().fieldInfo("_primary_term") != null;
            if (hasRootMarker) {
                var roots = leaf.reader().getNumericDocValues("_primary_term");
                if (roots == null) throw new IOException("cannot decode root-document markers");
                var nonRoots = new BitSet(leaf.reader().maxDoc());
                nonRoots.set(0, leaf.reader().maxDoc());
                for (int doc = roots.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = roots.nextDoc())
                    nonRoots.clear(doc);
                for (int doc = nonRoots.nextSetBit(0); doc >= 0; doc = nonRoots.nextSetBit(doc + 1))
                    deleted.set(leaf.docBase + doc);
            }
            // Nested Lucene children are represented inside their parent's source JSON.
            // Exporting them separately invents extra rows (or triggers missing-source failures).
            for (String field : new String[] { "_nested_path", "_type" }) {
                if (hasRootMarker) break;
                var terms = leaf.reader().terms(field);
                if (terms == null) {
                    if (leaf.reader().getFieldInfos().fieldInfo(field) != null)
                        throw new IOException("cannot decode legacy nested-document markers");
                    continue;
                }
                var iterator = terms.iterator();
                org.apache.lucene.util.BytesRef term;
                while ((term = iterator.next()) != null) {
                    if (field.equals("_type") && !term.utf8ToString().startsWith("__")) continue;
                    var postings = iterator.postings(null);
                    for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc())
                        deleted.set(leaf.docBase + doc);
                }
            }
            var live = leaf.reader().getLiveDocs();
            if (live != null) {
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    if (!live.get(doc)) deleted.set(leaf.docBase + doc);
                }
            }
            String softDeleteField = leaf.reader().getFieldInfos().getSoftDeletesField();
            if (softDeleteField == null && leaf.reader().getFieldInfos().fieldInfo("__soft_deletes") != null)
                softDeleteField = "__soft_deletes";
            if (softDeleteField != null) {
                var softDeletes = leaf.reader().getNumericDocValues(softDeleteField);
                if (softDeletes == null) throw new IOException("cannot decode soft-delete markers");
                for (int doc = softDeletes.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = softDeletes.nextDoc()) {
                    deleted.set(leaf.docBase + doc);
                }
            }
        }
        return deleted;
    }
}
