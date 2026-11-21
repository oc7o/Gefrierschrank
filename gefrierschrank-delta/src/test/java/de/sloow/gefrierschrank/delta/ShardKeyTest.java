package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ShardKeyTest {

    @Test
    void sameIndexShardAndSnapshotProduceTheSameKey() {
        assertEquals(
            ShardKey.of("my_index", 3, "snap_1"),
            ShardKey.of("my_index", 3, "snap_1"));
    }

    @Test
    void differentSnapshotOfTheSameIndexAndShardProducesADifferentKey() {
        String resumedFromOldSnapshot = ShardKey.of("my_index", 3, "snap_1");
        String freshRunAgainstNewerSnapshot = ShardKey.of("my_index", 3, "snap_2");

        assertNotEquals(resumedFromOldSnapshot, freshRunAgainstNewerSnapshot,
            "a shard from a different ES snapshot must not be mistaken for already-committed work");
    }

    @Test
    void differentShardIdProducesADifferentKey() {
        assertNotEquals(
            ShardKey.of("my_index", 0, "snap_1"),
            ShardKey.of("my_index", 1, "snap_1"));
    }

    @Test
    void differentIndexNameProducesADifferentKey() {
        assertNotEquals(
            ShardKey.of("index_a", 0, "snap_1"),
            ShardKey.of("index_b", 0, "snap_1"));
    }
}
