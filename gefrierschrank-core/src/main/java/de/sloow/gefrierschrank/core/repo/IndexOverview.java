package de.sloow.gefrierschrank.core.repo;

import java.util.List;

public record IndexOverview(String name, String repoUuid, List<String> shardGenerations, List<String> snapshotUuids) {}
