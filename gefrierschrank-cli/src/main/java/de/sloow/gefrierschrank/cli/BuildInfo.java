package de.sloow.gefrierschrank.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Identifies the running build, so a pasted run log shows which jar produced it. */
final class BuildInfo {

    static final String UNKNOWN_VERSION = "unknown-version";

    private BuildInfo() {}

    static String describe() {
        Properties properties = properties();
        return "gefrierschrank " + properties.getProperty("version", UNKNOWN_VERSION)
            + " (built " + properties.getProperty("built", "unknown") + ")";
    }

    /** The Maven project version this build was made from; companion jars carry it in their names. */
    static String version() {
        return properties().getProperty("version", UNKNOWN_VERSION);
    }

    private static Properties properties() {
        Properties properties = new Properties();
        try (InputStream in = BuildInfo.class.getResourceAsStream("/gefrierschrank-build.properties")) {
            if (in != null) properties.load(in);
        } catch (IOException ignored) {
            // an unidentified build is reported by the callers' defaults
        }
        return properties;
    }
}
