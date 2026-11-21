package de.sloow.gefrierschrank.core.repo;

import java.util.List;

public record RepositoryOverview(long latestGeneration, String minVersion, List<SnapshotOverview> snapshots, List<IndexOverview> indices) {}
