package com.mongodb.stats;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "currentOpAnalyzer", mixinStandardHelpOptions = true, version = "schemaAnalyzer 0.1")
public class CurrentOpAnalyzer implements Callable<Integer> {
	
	private static Logger logger = LoggerFactory.getLogger(CurrentOpAnalyzer.class);
	
	private final static Set<String> ignoreOps = new HashSet<>(Arrays.asList("hello", "isMaster", "ismaster"));
	
	
	@Option(names = {"--uri"}, description = "mongodb uri connection string", required = true)
    private String uri;

	@Option(names = "--i", description = "include idle operations")
    boolean idle;

	@Option(names = "--d", description = "discover toplogy hosts")
    boolean discover;

	@Option(names = "--connsByAppName", description = "group connections by application name")
    boolean connsByAppName;

	@Option(names = "--connsByIp", description = "group connections by IP address")
    boolean connsByIp;

	@Option(names = "--filterAppName", description = "filter by specific application name")
    String filterAppName;
	
	
	List<Document> pipeline = new ArrayList<>(1);
	
	
	
	public CurrentOpAnalyzer() {
		Document options = new Document("allUsers", true);
		options.append("idleConnections", true);
		options.append("idleCursors", true);
		options.append("idleSessions", true);
		options.append("localOps", true);

		Document currentOpPipeline = new Document("$currentOp", options);

		List<String> nins = new ArrayList<>();
		// "admin.$cmd","","local.oplog.rs"
		nins.add("");
		nins.add("admin.$cmd");
		nins.add("local.oplog.rs");

		Document nin  = new Document("$nin", nins);
		Document match = new Document("ns", nin);

		Document dollarMatch = new Document("$match", match);
		pipeline.add(currentOpPipeline);
		pipeline.add(dollarMatch);
	}
	
	private ShardClient shardClient;
	
	private void connect() {
		shardClient = new ShardClient("source", uri);
		shardClient.init();
		shardClient.populateShardMongoClients();
	}
	
	private String getStringValue(RawBsonDocument result, String key) {
		if (result != null && result.containsKey(key)) {
			BsonString bs = result.getString(key);
			if (bs != null) {
				return bs.getValue();
			}
		}
		return null;
	}
	
	private static class ConnectionInfo {
		String appName;
		String ipAddress;
		String driver;
		boolean active;
		Long secsRunning;

		public ConnectionInfo(String appName, String ipAddress, String driver, boolean active, Long secsRunning) {
			this.appName = appName != null ? appName : "(no appName)";
			this.ipAddress = ipAddress != null ? ipAddress : "(no IP)";
			this.driver = driver != null ? driver : "(no driver info)";
			this.active = active;
			this.secsRunning = secsRunning;
		}
	}

	private static class ConnectionStats {
		int totalCount = 0;
		int activeCount = 0;
		int inactiveCount = 0;
		long totalSecsRunning = 0;
		int secsRunningCount = 0;
		long maxSecsRunning = 0;

		void addConnection(ConnectionInfo conn) {
			totalCount++;
			if (conn.active) {
				activeCount++;
				if (conn.secsRunning != null) {
					totalSecsRunning += conn.secsRunning;
					secsRunningCount++;
					maxSecsRunning = Math.max(maxSecsRunning, conn.secsRunning);
				}
			} else {
				inactiveCount++;
			}
		}

		double getAverageSecsRunning() {
			return secsRunningCount > 0 ? (double) totalSecsRunning / secsRunningCount : 0.0;
		}
	}

	private List<ConnectionInfo> collectConnectionInfo(MongoClient mongoClient) {
		List<ConnectionInfo> connections = new ArrayList<>();
		MongoDatabase db = mongoClient.getDatabase("admin");
		AggregateIterable<RawBsonDocument> it = db.aggregate(pipeline, RawBsonDocument.class);

		for (RawBsonDocument result : it) {
			String desc = getStringValue(result, "desc");

			if (desc == null || !desc.startsWith("conn")) {
				continue;
			}

			// Extract client IP
			String client = getStringValue(result, "client");
			String ipAddress = null;
			if (client != null && client.contains(":")) {
				ipAddress = client.substring(0, client.lastIndexOf(":"));
			}

			// Extract app name
			String appName = getStringValue(result, "appName");

			// Extract driver info
			String driverInfo = null;
			RawBsonDocument clientMeta = (RawBsonDocument)result.get("clientMetadata");
			if (clientMeta != null) {
				RawBsonDocument driver = (RawBsonDocument)clientMeta.get("driver");
				if (driver != null) {
					String driverName = getStringValue(driver, "name");
					String driverVersion = getStringValue(driver, "version");
					if (driverName != null) {
						driverInfo = driverName;
						if (driverVersion != null) {
							driverInfo += " " + driverVersion;
						}
					}
				}
			}

			// Extract active status
			boolean active = false;
			if (result.containsKey("active") && result.getBoolean("active") != null) {
				active = result.getBoolean("active").getValue();
			}

			// Extract secs_running (only if active)
			Long secsRunning = null;
			if (active && result.containsKey("secs_running")) {
				BsonInt64 secsRunningVal = result.getInt64("secs_running");
				if (secsRunningVal != null) {
					secsRunning = secsRunningVal.longValue();
				}
			}

			connections.add(new ConnectionInfo(appName, ipAddress, driverInfo, active, secsRunning));
		}

		return connections;
	}

	private void printTable(String title, Map<String, ConnectionStats> data) {
		System.out.println("\n" + title);

		// Calculate max key length for formatting
		int maxKeyLength = Math.max(40, data.keySet().stream()
			.mapToInt(String::length)
			.max()
			.orElse(40));

		String keyHeader = title.contains("Application") ? "Application Name" :
		                   title.contains("IP") ? "IP Address" : "Driver";

		// Print header
		System.out.printf("%-" + maxKeyLength + "s  %6s  %6s  %8s  %10s  %10s%n",
			keyHeader, "Total", "Active", "Inactive", "AvgSecs", "MaxSecs");
		System.out.println("=".repeat(maxKeyLength + 50));

		// Sort by total count descending
		data.entrySet().stream()
			.sorted(Map.Entry.<String, ConnectionStats>comparingByValue(
				Comparator.comparingInt(s -> -s.totalCount)))
			.forEach(entry -> {
				String key = entry.getKey();
				if (key.length() > maxKeyLength) {
					key = key.substring(0, maxKeyLength - 3) + "...";
				}
				ConnectionStats stats = entry.getValue();
				System.out.printf("%-" + maxKeyLength + "s  %6d  %6d  %8d  %10.1f  %10d%n",
					key,
					stats.totalCount,
					stats.activeCount,
					stats.inactiveCount,
					stats.getAverageSecsRunning(),
					stats.maxSecsRunning);
			});

		System.out.println("=".repeat(maxKeyLength + 50));

		// Calculate totals
		ConnectionStats totals = new ConnectionStats();
		data.values().forEach(stats -> {
			totals.totalCount += stats.totalCount;
			totals.activeCount += stats.activeCount;
			totals.inactiveCount += stats.inactiveCount;
			totals.totalSecsRunning += stats.totalSecsRunning;
			totals.secsRunningCount += stats.secsRunningCount;
			totals.maxSecsRunning = Math.max(totals.maxSecsRunning, stats.maxSecsRunning);
		});

		System.out.printf("%-" + maxKeyLength + "s  %6d  %6d  %8d  %10.1f  %10d%n",
			"TOTAL",
			totals.totalCount,
			totals.activeCount,
			totals.inactiveCount,
			totals.getAverageSecsRunning(),
			totals.maxSecsRunning);
	}

	private void analyzeConnections(MongoClient mongoClient) {
		// Print timestamp in UTC
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String timestamp = ZonedDateTime.now(ZoneOffset.UTC).format(formatter);
		System.out.println("Timestamp (UTC): " + timestamp);

		List<ConnectionInfo> connections = collectConnectionInfo(mongoClient);

		if (connsByAppName) {
			// Group by application name
			Map<String, ConnectionStats> appNameStats = new HashMap<>();
			for (ConnectionInfo conn : connections) {
				appNameStats.computeIfAbsent(conn.appName, k -> new ConnectionStats())
					.addConnection(conn);
			}
			printTable("=== Connection Count by Application Name ===", appNameStats);
		}

		if (connsByIp) {
			// Filter by appName if specified
			List<ConnectionInfo> filtered = connections;
			String filterDesc = "";
			if (filterAppName != null) {
				filtered = connections.stream()
					.filter(c -> c.appName.equals(filterAppName))
					.collect(Collectors.toList());
				filterDesc = " (filtered by appName: " + filterAppName + ")";
			}

			// Group by IP address
			Map<String, ConnectionStats> ipStats = new HashMap<>();
			for (ConnectionInfo conn : filtered) {
				ipStats.computeIfAbsent(conn.ipAddress, k -> new ConnectionStats())
					.addConnection(conn);
			}
			printTable("=== Connection Count by IP Address" + filterDesc + " ===", ipStats);

			// Group by driver
			Map<String, ConnectionStats> driverStats = new HashMap<>();
			for (ConnectionInfo conn : filtered) {
				driverStats.computeIfAbsent(conn.driver, k -> new ConnectionStats())
					.addConnection(conn);
			}
			printTable("=== Connection Count by Driver" + filterDesc + " ===", driverStats);
		}
	}
	
	
	private void analyze() throws IOException {
		if (connsByAppName || connsByIp) {
			// Run once and exit for connection analysis
			if (shardClient.isMongos()) {
				Collection<MongoClient> mongoClients = shardClient.getMongosMongoClients();
				for (MongoClient mc : mongoClients) {
					analyzeConnections(mc);
				}
			} else {
				MongoClient mc = shardClient.getMongoClient();
				analyzeConnections(mc);
			}
		}
	}
	
	
	@Override
    public Integer call() throws Exception { 
        
		connect();
		analyze();
		
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new CurrentOpAnalyzer()).execute(args);
        System.exit(exitCode);
    }

}
