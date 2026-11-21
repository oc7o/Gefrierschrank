package de.sloow.gefrierschrank.core.repo;

/**
 * One file of a shard snapshot. {@code blobName} is the repository key (obfuscated
 * {@code __<uuid>} for stored data files, {@code v__<uuid>} for files embedded via
 * {@code meta_hash} instead of being uploaded as blobs).
 */
public record RepoFileInfo(
    String blobName,
    String physicalName,
    long length,
    String checksum,
    String writtenBy,
    String metaHash,
    long partSize
) {
    public RepoFileInfo(String blobName, String physicalName, long length, String checksum,
                        String writtenBy, String metaHash) {
        this(blobName, physicalName, length, checksum, writtenBy, metaHash, 0);
    }

    public int partCount() {
        return partSize <= 0 || length == 0 ? 1 : Math.toIntExact(1 + (length - 1) / partSize);
    }

    public String partName(int part) {
        return partCount() == 1 ? blobName : blobName + ".part" + part;
    }
}
