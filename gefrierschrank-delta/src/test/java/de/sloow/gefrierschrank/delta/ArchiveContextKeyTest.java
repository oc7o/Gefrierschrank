package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ArchiveContextKeyTest {

    @Test
    void parseKeyRoundTripsPlainAndMountedContextsIncludingTheMetadataShard() throws Exception {
        var plain = new ArchiveContext("logs", "repo-index-id", "snap-uuid", "snap-name", "logs", "snap-uuid");
        var mounted = new ArchiveContext("partial-logs", "shell", "mount-snapshot", "mount-backup",
            "logs", "backing-uuid");
        for (var context : new ArchiveContext[]{plain, mounted}) {
            for (int shard : new int[]{-1, 0, 7, Integer.MAX_VALUE}) {
                var parts = ArchiveContext.parseKey(context.key(shard)).orElseThrow();
                assertEquals(new ArchiveContext.KeyParts(context.repositoryIndexId(), context.snapshotUuid(),
                    context.indexName(), shard, context.sourceIndex(), context.sourceSnapshotUuid()), parts);
            }
        }
    }

    @Test
    void parseKeyRoundTripsNamesThatNeedJsonEscaping() throws Exception {
        var context = new ArchiveContext("we\"ird\\name", "id", "uuid", "n", "srcä", "uuid");
        assertEquals("we\"ird\\name", ArchiveContext.parseKey(context.key(3)).orElseThrow().indexName());
    }

    @Test
    void parseKeyRejectsLegacyAndMalformedKeys() {
        for (String key : new String[]{
            null,
            ShardKey.of("i", 0, "s"),
            "[\"r\",\"s\",\"i\",0,\"i\",\"s\"]",
            "archive-v2:[\"r\",\"s\",\"i\",0,\"i\",\"s\"]",
            "archive-v1:{\"r\":\"s\"}",
            "archive-v1:\"r\"",
            "archive-v1:",
            "archive-v1:[\"r\",\"s\",\"i\",0,\"i\"]",
            "archive-v1:[\"r\",\"s\",\"i\",0,\"i\",\"s\",\"x\"]",
            "archive-v1:[\"r\",\"s\",\"i\",\"0\",\"i\",\"s\"]",
            "archive-v1:[\"r\",\"s\",\"i\",0.0,\"i\",\"s\"]",
            "archive-v1:[\"r\",\"s\",\"i\",4294967296,\"i\",\"s\"]",
            "archive-v1:[\"r\",null,\"i\",0,\"i\",\"s\"]",
            "archive-v1:[\"r\",1,\"i\",0,\"i\",\"s\"]",
            "archive-v1:[\"r\", \"s\",\"i\",0,\"i\",\"s\"]",
            "archive-v1:[\"r\",\"s\",\"i\",0,\"i\",\"s\"] trailing",
            "archive-v1:[\"r\",\"s\",\"i\",0,\"i\",\"s\""}) {
            assertEquals(Optional.empty(), ArchiveContext.parseKey(key), String.valueOf(key));
        }
    }
}
