package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ArchiveSourceTest {
    @Test
    void retainsExactSourcesAcrossShapesAndShards(@TempDir Path target) throws Exception {
        var context = new ArchiveContext("index", "repo-index", "uuid", "snap", "index", "uuid");
        List<String> sources = List.of(
            "{\n \"value\": 18446744073709551615, \"decimal\": 0.12345678901234567890123456789, \"@x\":1,\"_x\":2}",
            "{\"value\":[1,2,null],\"later\":{\"items\":[{\"a\":1}]},\"_unmapped_fields\":\"original\"}",
            "{\"value\":\"changed type\",\"_source\":{\"original\":true}}");
        var writer = new DeltaTableWriter(target.toString());
        var conf = new Configuration();
        for (int shard = 0; shard < sources.size(); shard++) {
            String row = context.document(sources.get(shard), shard, new DocumentMetadata("id" + shard, "route", "stored", 4));
            writer.write(conf, List.of(row).iterator(), ArchiveContext.mapping(), context.key(shard));
        }
        var actual = new java.util.HashSet<String>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(conf, target.toString()).snapshot().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                actual.add(row.getString("_source"));
                assertEquals("route", row.getString("_routing"));
                assertEquals(4, row.getLong("_doc"));
            }
        }
        assertEquals(new java.util.HashSet<>(sources), actual);
        writer.requireArchiveSnapshot(conf, context, 3);
    }

    @Test
    void emptySnapshotIsCommittedAndIncompatibleTargetIsRejected(@TempDir Path target) throws Exception {
        var conf = new Configuration();
        var context = new ArchiveContext("i", "r", "s", "snap", "i", "s");
        var writer = new DeltaTableWriter(target.resolve("empty").toString());
        assertEquals(0, writer.write(conf, List.<String>of().iterator(), ArchiveContext.mapping(), context.key(0)));
        writer.requireArchiveSnapshot(conf, context, 1);
        var old = new DeltaTableWriter(target.resolve("legacy").toString());
        old.write(List.of("{\"value\":42}").iterator());
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class, () -> old.requireArchiveSnapshot(conf, context, 1));
    }

    @Test
    void refusesMissingAndMalformedSource() {
        var context = new ArchiveContext("i", "r", "s", "snap", "i", "s");
        var metadata = new DocumentMetadata(null, null, "stored", 0);
        for (String source : new String[]{null, " ", "[]", "{} {}", "{\"value\":"})
            assertThrows(java.io.IOException.class, () -> context.document(source, 0, metadata));
    }
}
