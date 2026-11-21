package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Real-world shape: _source documents with nested objects, arrays, nulls. */
class EsDocumentShapeTest {

    @Test
    void handlesNestedObjectsAndArrays(@TempDir Path target) throws Exception {
        List<String> docs = List.of(
            "{\"id\": 1, \"title\": \"P1\", \"tags\": [\"a\",\"b\"], \"meta\": {\"origin\": \"es9\"}, \"active\": true}",
            "{\"id\": 2, \"title\": \"P2\", \"tags\": [\"b\"], \"meta\": {\"origin\": \"es9\"}, \"price\": 3.5}");

        var tablePath = target.resolve("t");
        long count = new DeltaTableWriter(tablePath.toUri().toString()).write(docs.iterator());
        assertEquals(2, count);

        var conf = new org.apache.hadoop.conf.Configuration();
        var log = io.delta.standalone.DeltaLog.forTable(conf, tablePath.toString());
        try (var it = log.snapshot().open()) {
            int seen = 0;
            long[] ids = new long[2];
            while (it.hasNext()) {
                var record = it.next();
                ids[seen] = record.getLong("id");
                seen++;
            }
            assertEquals(2, seen);
            assertEquals(1L, ids[0]);
        }
    }

    @Test
    void rejectsEmptyInput(@TempDir Path target) {
        var tablePath = target.resolve("empty");
        assertThrows(Exception.class,
            () -> new DeltaTableWriter(tablePath.toUri().toString()).write(List.<String>of().iterator()));
    }
}
