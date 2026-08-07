package com.mongodb.shardsync.command;

import java.io.InputStream;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import picocli.CommandLine.IVersionProvider;

/**
 * Supplies the --version output from the jar manifest's Implementation-Version,
 * which is populated from ${project.version} at build time. This avoids keeping
 * a version string in the source that has to be bumped alongside the pom.
 */
public class ManifestVersionProvider implements IVersionProvider {

    private static final String UNKNOWN = "unknown (not running from a packaged jar)";

    @Override
    public String[] getVersion() {
        return new String[] { "mongo-util " + resolveVersion() };
    }

    private String resolveVersion() {
        // Works when the class was loaded from a jar whose manifest defines the package version.
        String version = ManifestVersionProvider.class.getPackage().getImplementationVersion();
        if (version != null && !version.isBlank()) {
            return version.trim();
        }

        // Fall back to reading the manifest of the jar this class came from directly.
        try {
            URL location = ManifestVersionProvider.class.getResource("ManifestVersionProvider.class");
            if (location != null && "jar".equals(location.getProtocol())) {
                String jarUrl = location.toString();
                URL manifestUrl = new URL(jarUrl.substring(0, jarUrl.indexOf("!") + 1) + "/META-INF/MANIFEST.MF");
                try (InputStream in = manifestUrl.openStream()) {
                    Attributes attrs = new Manifest(in).getMainAttributes();
                    String value = attrs.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
                    if (value != null && !value.isBlank()) {
                        return value.trim();
                    }
                }
            }
        } catch (Exception e) {
            // fall through to UNKNOWN
        }
        return UNKNOWN;
    }
}
