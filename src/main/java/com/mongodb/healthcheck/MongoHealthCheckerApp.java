package com.mongodb.healthcheck;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * MongoDB Health Checker - Analyzes index usage across a cluster
 *
 * TODO: Refactor into separate picocli commands:
 *   1. "collect" command - Collects index statistics and saves to JSON file
 *      - Faster iteration on report generation without re-collecting data
 *      - Enables programmatic processing of collected data
 *      - Example: healthcheck collect --uri <uri> --output data.json
 *   2. "report" command - Generates HTML report from collected JSON data
 *      - Example: healthcheck report --input data.json --output report.html
 *   This separation allows testing report changes without waiting for data collection
 */
@Command(name = "mongoHealthChecker",
         mixinStandardHelpOptions = true,
         description = "Generates an HTML report of index usage statistics across a MongoDB cluster")
public class MongoHealthCheckerApp implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(MongoHealthCheckerApp.class);

    @Option(names = {"--uri"}, required = true,
            description = "MongoDB connection URI")
    private String uri;

    @Option(names = {"--output", "-o"},
            description = "Output HTML file path (default: mongo-health-report.html)")
    private String outputPath = "mongo-health-report.html";

    @Option(names = {"--lowUsageThreshold"},
            description = "Threshold for low usage index accesses (default: 100)")
    private int lowUsageThreshold = 100;

    @Override
    public Integer call() throws Exception {
        logger.info("Starting MongoDB Health Check");
        logger.info("Connecting to: {}", sanitizeUri(uri));

        try (MongoClient client = MongoClients.create(uri)) {
            logger.info("Connected successfully");

            // Collect index statistics
            IndexStatsCollector collector = new IndexStatsCollector(client, lowUsageThreshold);
            ClusterIndexStats stats = collector.collect();

            // Generate HTML report
            logger.info("Generating HTML report...");
            HealthReportGenerator generator = new HealthReportGenerator();
            String html = generator.generateReport(stats);

            // Write report to file
            File outputFile = new File(outputPath);
            try (FileWriter writer = new FileWriter(outputFile)) {
                writer.write(html);
            }

            logger.info("Health check report generated: {}", outputFile.getAbsolutePath());
            logger.info("Total indexes: {}", stats.getTotalIndexCount());
            logger.info("Unused indexes: {}", stats.getUnusedIndexCount());
            logger.info("Low usage indexes: {}", stats.getLowUsageIndexCount());

            return 0;
        } catch (Exception e) {
            logger.error("Error during health check", e);
            return 1;
        }
    }

    private String sanitizeUri(String uri) {
        // Remove credentials from URI for logging
        return uri.replaceAll("://[^@]+@", "://<credentials>@");
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MongoHealthCheckerApp()).execute(args);
        System.exit(exitCode);
    }
}
