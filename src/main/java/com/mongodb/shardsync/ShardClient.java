package com.mongodb.shardsync;

import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.internal.dns.DefaultDnsResolver;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.model.DatabaseCatalogProvider;
import com.mongodb.model.IndexSpec;
import com.mongodb.model.Mongos;
import com.mongodb.model.Namespace;
import com.mongodb.model.Privilege;
import com.mongodb.model.ReplicaSetInfo;
import com.mongodb.model.Resource;
import com.mongodb.model.Role;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.model.StandardDatabaseCatalogProvider;
import com.mongodb.model.User;
import com.mongodb.util.MaskUtil;
import com.mongodb.util.bson.BsonUuidUtil;

/**
 * This class encapsulates the client related objects needed for each source and
 * destination
 *
 */
public class ShardClient {

	public enum ShardClientType {
		SHARDED_NO_SRV,
		SHARDED,
		REPLICA_SET
	}

	private static Logger logger = LoggerFactory.getLogger(ShardClient.class);
	private static Pattern excludeCollRegex = Pattern.compile("system\\..*");

	private final static int ONE_GIGABYTE = 1024 * 1024 * 1024;
	private final static int ONE_MEGABYTE = 1024 * 1024;

	private DocumentCodec codec = new DocumentCodec();

	private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";

	private final static List<Document> countPipeline = new ArrayList<Document>();
	static {
		countPipeline.add(Document.parse("{ $group: { _id: null, count: { $sum: 1 } } }"));
		countPipeline.add(Document.parse("{ $project: { _id: 0, count: 1 } }"));
	}

	private final static Document listDatabasesCommand = new Document("listDatabases", 1);
	private final static Document dbStatsCommand = new Document("dbStats", 1);

	public final static Set<String> excludedSystemDbs = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));

	private ShardClientType shardClientType = ShardClientType.SHARDED;
	private String name;
	private String version;
	private List<Integer> versionArray;
	// private MongoClientURI mongoClientURI;
	private MongoClient mongoClient;
	private MongoDatabase configDb;
	private Map<String, Shard> shardsMap = new LinkedHashMap<String, Shard>();

	private Map<String, RawBsonDocument> chunksCache = new LinkedHashMap<>();

	private Map<String, Shard> tertiaryShardsMap = new LinkedHashMap<String, Shard>();
	Map<String, String> rsNameToShardIdMap = new HashMap<>();

	private ConnectionString connectionString;
	private MongoClientSettings mongoClientSettings;

	private CodecRegistry pojoCodecRegistry;

	private ConnectionString csrsConnectionString;
	private MongoClientSettings csrsMongoClientSettings;
	private MongoClient csrsMongoClient;

	// private String username;
	// private String password;
	// private MongoClientOptions mongoClientOptions;

	private List<Mongos> mongosList = new ArrayList<Mongos>();
	private Map<String, MongoClient> mongosMongoClients = new TreeMap<String, MongoClient>();

	private Map<String, Document> collectionsMap = new TreeMap<String, Document>();
	private Map<UUID, String> collectionsUuidMap = new TreeMap<>();
	private DatabaseCatalog databaseCatalog;
	private DatabaseCatalogProvider databaseCatalogProvider;

	private Map<String, MongoClient> shardMongoClients = new TreeMap<String, MongoClient>();

	private List<String> srvHosts;

	private Collection<String> shardIdFilter;

	private Boolean rsSsl;
	private boolean patternedUri;
	private boolean manualShardHosts;
	private boolean mongos;
	private String connectionStringPattern;
	private String rsPattern;
	private String csrsUri;
	private String rsRegex;

	// Advanced only, for manual configuration / overriding discovery
	private String[] rsStringsManual;

	public ShardClient(String name, String clusterUri, Collection<String> shardIdFilter, ShardClientType shardClientType) {

		this.patternedUri = clusterUri.contains("%s");
		if (patternedUri) {
			this.connectionStringPattern = clusterUri;
			// example
			// mongodb://admin@cluster1-%s-%s-%s.wxyz.mongodb.net:27017/?ssl=true&authSource=admin
			String csrsUri = String.format(clusterUri, "config", 0, 0);
			logger.debug(name + " csrsUri from pattern: " + csrsUri);
			this.connectionString = new ConnectionString(csrsUri);
		} else {
			this.connectionString = new ConnectionString(clusterUri);
		}
		this.shardClientType = shardClientType;

		if (ShardClientType.SHARDED_NO_SRV.equals(shardClientType)  && connectionString.isSrvProtocol()) {
			throw new IllegalArgumentException(
					"srv protocol not supported, please configure a single mongos mongodb:// connection string");
		}

		this.name = name;
		this.shardIdFilter = shardIdFilter;
		logger.info(String.format("%s client, uri: %s", name, MaskUtil.maskConnectionString(connectionString)));
	}

	public ShardClient(String name, String clusterUri) {
		this(name, clusterUri, null, null);
	}

	public void init() {
		this.manualShardHosts = rsStringsManual != null && rsStringsManual.length > 0;

		if (csrsUri != null) {
			logger.debug(name + " csrsUri: " + csrsUri);
			this.csrsConnectionString = new ConnectionString(csrsUri);
			this.csrsMongoClientSettings = MongoClientSettings.builder()
					.applyConnectionString(csrsConnectionString)
					.uuidRepresentation(UuidRepresentation.STANDARD)
					.build();
			this.csrsMongoClient = MongoClients.create(csrsMongoClientSettings);
		}
		mongoClientSettings = MongoClientSettings.builder()
				.applyConnectionString(connectionString)
				.uuidRepresentation(UuidRepresentation.STANDARD)
				.build();
		mongoClient = MongoClients.create(mongoClientSettings);

		pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
				fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		databaseCatalogProvider = new StandardDatabaseCatalogProvider(mongoClient);

		try {
			Document dbgridResult = adminCommand(new Document("isdbgrid", 1));
			Integer dbgrid = dbgridResult.getInteger("isdbgrid");
			mongos = dbgrid.equals(1);
		} catch (MongoException mce) {
			// ignore not supported
		}

		configDb = mongoClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
		
		if (mongos) {
			populateShardList();
		} else {
			populateShardListNotSharded();
		}
		
		initVersionArray();
		populateMongosList();
		
		if (this.isVersion5OrLater()) {
			populateCollectionsMap();
		}
	}
	
	public void initVersionArray() {
		Document destBuildInfo = adminCommand(new Document("buildinfo", 1));
		version = destBuildInfo.getString("version");
		versionArray = (List<Integer>) destBuildInfo.get("versionArray");
		logger.info(String.format("%s : MongoDB version: %s, mongos: %s", name, version, mongos));
	}

	/**
	 * If we're using patternedUri, we assume that the source or dest is an Atlas
	 * cluster that's in the midst of LiveMigrate and this tool is being used to
	 * reverse sync. In that case the data that's in the config server is WRONG.
	 * But, we'll use that to assume the number of shards (for now)
	 *
	 */
	private void populateShardList() {

		MongoCollection<Shard> shardsColl = configDb.getCollection("shards", Shard.class);
		FindIterable<Shard> shards = shardsColl.find().sort(Sorts.ascending("_id"));

		if (rsRegex != null) {
			logger.debug("{}: populateShardList(), rsRegex: {}", name, rsRegex);
			Pattern p = Pattern.compile(rsRegex);
			for (Shard sh : shards) {

				String seedList = StringUtils.substringAfter(sh.getHost(), "/");
				String rsName = StringUtils.substringBefore(sh.getHost(), "/");
				sh.setRsName(rsName);

				ReplicaSetInfo rsInfo = getReplicaSetInfoFromHost(seedList);

				if (rsInfo == null) {
					throw new RuntimeException("unable to get rsInfo from " + seedList);
				}
				
				String foundHost = null;
				for (String host : rsInfo.getHosts()) {

					if (p.matcher(host).find()) {
						rsInfo = getReplicaSetInfoFromHost(host);
						// connection may have failed
						if (rsInfo == null) {
							continue;
						}
						logger.debug("match, rs: {}, host: {}, secondary: {}", rsName, host, rsInfo.isSecondary());
						if (rsInfo.isSecondary()) {
							foundHost = host;
							break;
						}
					} else {
						logger.debug("no match, rs: {}, host: {}", rsName, host);
					}
				}

				if (foundHost == null) {
					throw new IllegalArgumentException(String.format("Unable to find matching host for regex %s, seedList: %s", rsRegex, seedList));
				}
				sh.setHost(foundHost);

				logger.debug("regex host: {}, secondary: {}", foundHost, rsInfo.isSecondary());


				if (this.shardIdFilter == null) {
					shardsMap.put(sh.getId(), sh);
				} else if (shardIdFilter.contains(sh.getId())) {
					shardsMap.put(sh.getId(), sh);
				}
			}
		} else {

			for (Shard sh : shards) {

				if (!patternedUri && !manualShardHosts) {
					logger.debug(String.format("%s: populateShardList shard: %s", name, sh.getHost()));
				}
				String rsName = StringUtils.substringBefore(sh.getHost(), "/");
				sh.setRsName(rsName);
				if (this.shardIdFilter == null) {
					shardsMap.put(sh.getId(), sh);
				} else if (shardIdFilter.contains(sh.getId())) {
					shardsMap.put(sh.getId(), sh);
				}
			}

			if (patternedUri) {
				int shardCount = shardsMap.size();
				tertiaryShardsMap.putAll(shardsMap);
				shardsMap.clear();
				for (int shardNum = 0; shardNum < shardCount; shardNum++) {

					String hostBasePre = StringUtils.substringAfter(connectionStringPattern, "mongodb://");
					String hostBase = StringUtils.substringBefore(hostBasePre, "/");
					if (hostBase.contains("@")) {
						hostBase = StringUtils.substringAfter(hostBase, "@");
					}
					String host0 = String.format(hostBase, "shard", shardNum, 0);
					String host1 = String.format(hostBase, "shard", shardNum, 1);
					String rsName = String.format(this.rsPattern, "shard", shardNum);
					Shard sh = new Shard();
					sh.setId(rsName);
					sh.setRsName(rsName);
					sh.setHost(String.format("%s/%s,%s", rsName, host0, host1));
					shardsMap.put(sh.getId(), sh);
					logger.debug(String.format("%s: populateShardList formatted shard name: %s", name, sh.getHost()));
				}

			} else if (manualShardHosts) {

				// in some cases the rs name doesn't match the shard name
				for (Shard shard : shardsMap.values()) {
					rsNameToShardIdMap.put(shard.getRsName(), shard.getId());
				}
				shardsMap.clear();

				for (String rsString : rsStringsManual) {

					Shard sh = new Shard();
					if (!rsString.contains("/")) {
						if (rsString.contains(",")) {
							throw new IllegalArgumentException(String.format("Invalid format for %sRsManual, expecting rsName/host1:port,host2:port,host3:port", name));
						} else {
							logger.warn(String.format("Config for %sRsManual is using standalone/direct connect", name));
							sh.setHost(rsString);
							String rsName = getReplicaSetInfoFromHost(rsString).getRsName();
							sh.setRsName(rsName);
							String shardId = rsNameToShardIdMap.get(rsName);
							sh.setId(shardId);
						}

					} else {
						String rsName = StringUtils.substringBefore(rsString, "/");
						sh.setRsName(rsName);
						sh.setId(rsName);
						sh.setHost(String.format("%s/%s", rsName, StringUtils.substringAfter(rsString, "/")));
					}

					shardsMap.put(sh.getId(), sh);
					logger.debug("{}: populateShardList added manual shard connection: {}", name, sh.getHost());
				}
			}
		}

		logger.debug(name + ": populateShardList complete, " + shardsMap.size() + " shards added");
	}
	
	private void populateShardListNotSharded() {
		Document result = adminCommand(new Document("replSetGetConfig", 1));
		if (result != null) {
			Document config = (Document)result.get("config");
			String id  = config.getString("_id");
			Shard shard = new Shard();
			shard.setId(id);
			shard.setRsName(id);
			shardsMap.put(id, shard);
		}
		
	}

	private ReplicaSetInfo getReplicaSetInfoFromHost(String host) {
		ReplicaSetInfo rsInfo = new ReplicaSetInfo();
		
		ConnectionString connectionString = new ConnectionString("mongodb://" + host);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToSocketSettings(builder -> {
                    builder.connectTimeout(5000, MILLISECONDS);
                  })
                .build();
		MongoClient tmp = MongoClients.create(mongoClientSettings);
		
		Document result = null;
		try {
			result = tmp.getDatabase("admin").runCommand(new Document("isMaster", 1));
		} catch (MongoException me) {
			logger.warn("Error getting isMaster() for {}, {}", host, me.getMessage());
			return null;
		}
		
		if (result.containsKey("setName")) {
			rsInfo.setRsName(result.getString("setName"));
		} else {
			logger.warn("Unable to get setName from isMaster result for host: {}", host);
		}

		rsInfo.setHosts(result.getList("hosts", String.class));
		rsInfo.setWritablePrimary(result.getBoolean("ismaster"));
		rsInfo.setSecondary(result.getBoolean("secondary"));
		tmp.close();
		return rsInfo;
	}

	private void populateMongosList() {

		if (patternedUri) {
			logger.debug("populateMongosList() skipping, patternedUri");
			return;
		}

		if (connectionString.isSrvProtocol()) {

			DefaultDnsResolver resolver = new DefaultDnsResolver();
			srvHosts = resolver.resolveHostFromSrvRecords(connectionString.getHosts().get(0), "mongodb");

			for (String hostPort : srvHosts) {
				logger.debug("populateMongosList() mongos srvHost: " + hostPort);

				String host = StringUtils.substringBefore(hostPort, ":");
				Integer port = Integer.parseInt(StringUtils.substringAfter(hostPort, ":"));

				MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
				settingsBuilder
						.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host, port))));
				if (connectionString.getSslEnabled() != null) {
					settingsBuilder.applyToSslSettings(builder -> builder.enabled(connectionString.getSslEnabled()));
				}
				if (connectionString.getCredential() != null) {
					settingsBuilder.credential(connectionString.getCredential());
				}
				settingsBuilder.uuidRepresentation(UuidRepresentation.STANDARD);
				MongoClientSettings settings = settingsBuilder.build();

				MongoClient mongoClient = MongoClients.create(settings);
				mongosMongoClients.put(hostPort, mongoClient);
			}

		} else {
			MongoCollection<Mongos> mongosColl = configDb.getCollection("mongos", Mongos.class);
			// LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);

			// TODO this needs to take into account "dead" mongos instances
			int limit = 100;
			if (name.equals("source")) {
				limit = 5;
			}
			mongosColl.find().sort(Sorts.ascending("ping")).limit(limit).into(mongosList);
			for (Mongos mongos : mongosList) {

				// logger.debug(name + " mongos: " + mongos.getId());

				String hostPort = mongos.getId();
				String host = StringUtils.substringBefore(hostPort, ":");
				Integer port = Integer.parseInt(StringUtils.substringAfter(hostPort, ":"));

				MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
				settingsBuilder
						.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host, port))));
				if (connectionString.getSslEnabled() != null) {
					settingsBuilder.applyToSslSettings(builder -> builder.enabled(connectionString.getSslEnabled()));
				}
				if (connectionString.getCredential() != null) {
					settingsBuilder.credential(connectionString.getCredential());
				}
				settingsBuilder.uuidRepresentation(UuidRepresentation.STANDARD);
				MongoClientSettings settings = settingsBuilder.build();
				MongoClient mongoClient = MongoClients.create(settings);
				mongosMongoClients.put(mongos.getId(), mongoClient);
			}

		}

		logger.debug(name + " populateMongosList complete, " + mongosMongoClients.size() + " mongosMongoClients added");
	}

	/**
	 * Populate only a subset of all collections. Useful if there are a very large
	 * number of namespaces present.
	 */
	public void populateCollectionsMap(boolean clearExisting, Set<String> namespaces) {
		if (clearExisting) {
			collectionsMap.clear();
		}

		if (!collectionsMap.isEmpty()) {
			return;
		}
		logger.debug("{} Starting populateCollectionsMap()", name);
		MongoCollection<Document> shardsColl = configDb.getCollection("collections");
		Bson filter = null;
		if (namespaces == null || namespaces.isEmpty()) {
			if (! isVersion5OrLater()) {
				filter = eq("dropped", false);
			}
		} else {
			if (! isVersion5OrLater()) {
				filter = in("_id", namespaces);
			} else {
				filter = and(eq("dropped", false), in("_id", namespaces));
			}
		}
		FindIterable<Document> colls; 
		if (filter != null) {
			colls = shardsColl.find(filter).sort(Sorts.ascending("_id"));
		} else {
			colls = shardsColl.find().sort(Sorts.ascending("_id"));
		}
		
		for (Document c : colls) {
			String id = (String) c.get("_id");
			collectionsMap.put(id, c);
			if (c.containsKey("uuid") && isVersion5OrLater()) {
				UUID uuid = (UUID)c.get("uuid");
				collectionsUuidMap.put(uuid, id);
			}
		}
		logger.debug(String.format("%s Finished populateCollectionsMap(), %s collections loaded from config server",
				name, collectionsMap.size()));
		((StandardDatabaseCatalogProvider)databaseCatalogProvider).setCollectionsMap(collectionsMap);
	}
	
	public void populateCollectionsMap(Set<String> namespaces) {
		populateCollectionsMap(false, namespaces);
	}

	public void populateCollectionsMap(boolean clearExisting) {
		populateCollectionsMap(clearExisting, null);
	}
	
	public void populateCollectionsMap() {
		populateCollectionsMap(false, null);
	}

	public void populateShardMongoClients() {
		// MongoCredential sourceCredentials = mongoClientURI.getCredentials();

		if (shardMongoClients.size() > 0) {
			logger.debug("populateShardMongoClients already complete, skipping");
			return;
		}
		
		if (! this.mongos) {
			shardMongoClients.put("", this.mongoClient);
			return;
		}

		for (Shard shard : shardsMap.values()) {
			String shardHost = shard.getHost();
			String seeds = StringUtils.substringAfter(shardHost, "/");

			MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
			List<ServerAddress> serverAddressList = new ArrayList<>();

			if (seeds.equals("")) {
				logger.debug(name + " " + shard.getId() + " populateShardMongoClients() no rs string provided");
			} else {
				logger.debug(name + " " + shard.getId() + " populateShardMongoClients() seeds: " + seeds);
			}



			if (seeds.contains(",")) {
				String[] seedHosts = seeds.split(",");
				for (String seed : seedHosts) {
					String host = StringUtils.substringBefore(seed, ":");
					Integer port = Integer.parseInt(StringUtils.substringAfter(seed, ":"));
					serverAddressList.add(new ServerAddress(host, port));
				}
			} else {
				String host = StringUtils.substringBefore(shardHost, ":");
				if (host.contains("/")) {
					String host1 = StringUtils.substringAfter(shardHost, "/");
					host = StringUtils.substringBefore(host1, ":");
				}
				logger.debug("{} - no seedlist seen, host: {}", name, host);
				Integer port = Integer.parseInt(StringUtils.substringAfter(shardHost, ":"));
				serverAddressList.add(new ServerAddress(host, port));
			}

			settingsBuilder.applyToClusterSettings(builder -> builder.hosts(serverAddressList));
			
			
			if (rsSsl != null) {
				logger.debug("manual rs ssl config: {}", rsSsl);
				settingsBuilder.applyToSslSettings(builder -> builder.enabled(rsSsl));
			} else if (connectionString.getSslEnabled() != null) {
				logger.debug("***** {} - SSL config set from connection string", name, rsSsl);
				settingsBuilder.applyToSslSettings(builder -> builder.enabled(connectionString.getSslEnabled()));
			}
			if (connectionString.getCredential() != null) {
				settingsBuilder.credential(connectionString.getCredential());
			}
			if (connectionString.getApplicationName() != null) {
				settingsBuilder.applicationName(connectionString.getApplicationName());
			}
			if (connectionString.getReadPreference() != null) {
				settingsBuilder.readPreference(connectionString.getReadPreference());
			}
			settingsBuilder.uuidRepresentation(UuidRepresentation.STANDARD);

            if (connectionString.getReadPreference() != null) {
				settingsBuilder.readPreference(connectionString.getReadPreference());
			}
			MongoClientSettings settings = settingsBuilder.build();
			MongoClient mongoClient = MongoClients.create(settings);

			shardMongoClients.put(shard.getId(), mongoClient);
			
			logger.debug("{} - clusterSettings: {}, sslSettings: {}", name, settings.getClusterSettings(), settings.getSslSettings());

			// logger.debug(String.format("%s isMaster started: %s", name, shardHost));
			Document isMasterResult = mongoClient.getDatabase("admin").runCommand(new Document("isMaster", 1));
			if (logger.isTraceEnabled()) {
				logger.trace(name + " isMaster complete, cluster: " + mongoClient.getClusterDescription());
			} else {
				// logger.debug(String.format("%s isMaster complete: %s", name, shardHost));
			}
		}
	}

	public Document getLatestOplogEntry(String shardId) {
		MongoClient client = shardMongoClients.get(shardId);
		MongoCollection<Document> coll = client.getDatabase("local").getCollection("oplog.rs");
		Document doc = coll.find(ne("op", "n")).sort(eq("$natural", -1)).first();
		return doc;
	}

	public BsonTimestamp getLatestOplogTimestamp(String shardId) {
		return getLatestOplogTimestamp(shardId, null);
	}

	public BsonTimestamp getLatestOplogTimestamp(String shardId, Bson query) {
		BsonDocument doc = getLatestOplogEntry(shardId, query);
		BsonTimestamp ts = doc.getTimestamp("ts");
		return ts;
	}
	
	public BsonDocument getLatestOplogEntry(String shardId, Bson query) {
		MongoClient client = shardMongoClients.get(shardId);
		if (client == null) {
			client = this.getMongoClient();
		}
		MongoCollection<BsonDocument> coll = client.getDatabase("local").getCollection("oplog.rs", BsonDocument.class);
		Bson proj = include("ts","t");
		Bson sort = eq("$natural", -1);
		BsonDocument doc = null;
		if (query != null) {
			doc = coll.find(query).comment("getLatestOplogTimestamp").projection(proj).sort(sort).first();
		} else {
			doc = coll.find().comment("getLatestOplogTimestamp").projection(proj).sort(sort).first();
		}
		return doc;
	}
	
	public ShardTimestamp populateLatestOplogTimestamp(Shard shard, String startingTs) {
		
		Bson query = getLatestOplogQuery(startingTs);
		String shardId = null;
		BsonDocument oplogEntry = null;
		if (shard != null) {
			shardId = shard.getId();
			oplogEntry = getLatestOplogEntry(shardId, query);
		} else {
			
			oplogEntry = getLatestOplogEntry(null, query);
		}
		logger.debug("latest oplog entry: {}", oplogEntry);
		ShardTimestamp st = new ShardTimestamp(shard, oplogEntry);
		shard.setSyncStartTimestamp(st);
		return st;
	}

	public ShardTimestamp populateLatestOplogTimestamp(String shardId, String startingTs) {
		Bson query = getLatestOplogQuery(startingTs);
		BsonDocument oplogEntry = getLatestOplogEntry(shardId, query);
		ShardTimestamp st = new ShardTimestamp(shardId, oplogEntry.getTimestamp("ts"));
		this.getShardsMap().get(shardId).setSyncStartTimestamp(st);
		return st;
	}
	
	private Bson getLatestOplogQuery(String startingTs) {
		Bson query = null;
		if (startingTs != null) {
			if (startingTs.contains(",")) {
				String[] tsParts = startingTs.split(",");
				int seconds = Integer.parseInt(tsParts[0]);
				int increment = Integer.parseInt(tsParts[1]);
				query = gt("ts", new BsonTimestamp(seconds, increment));
			} else {
				throw new IllegalArgumentException("Error parsing timestamp no comma found, expected format ts,increment");
			}

		}
		return query;
	}

	/**
	 * This will drop the db on each shard, config data will NOT be touched
	 *
	 * @param dbName
	 */
	public void dropDatabase(String dbName) {
		for (Map.Entry<String, MongoClient> entry : shardMongoClients.entrySet()) {
			logger.debug(name + " dropping " + dbName + " on " + entry.getKey());
			entry.getValue().getDatabase(dbName).drop();
		}
	}

	/**
	 * This will drop the db on each shard, config data will NOT be touched
	 *
	 * @param
	 */
	public void dropDatabases(List<String> databasesList) {
		for (Map.Entry<String, MongoClient> entry : shardMongoClients.entrySet()) {

			for (String dbName : databasesList) {
				if (!dbName.equals("admin")) {
					logger.debug(name + " dropping " + dbName + " on " + entry.getKey());
					entry.getValue().getDatabase(dbName).drop();
				}

			}
		}
	}

	private void dropForce(String dbName) {
		DeleteResult r = mongoClient.getDatabase("config").getCollection("collections")
				.deleteMany(regex("_id", "^" + dbName + "\\."));
		logger.debug(String.format("Force deleted %s config.collections documents", r.getDeletedCount()));
		r = mongoClient.getDatabase("config").getCollection("chunks").deleteMany(regex("ns", "^" + dbName + "\\."));
		logger.debug(String.format("Force deleted %s config.chunks documents", r.getDeletedCount()));
	}

	public void dropDatabasesAndConfigMetadata(Collection<String> databasesList) {
		for (String dbName : databasesList) {
			if (!dbName.equals("admin")) {
				logger.debug(name + " dropping " + dbName);
				try {
					mongoClient.getDatabase(dbName).drop();
				} catch (MongoCommandException mce) {
					logger.warn("Drop failed, brute forcing.", mce);
					dropForce(dbName);
				}

			}
		}
	}

	public static Number estimatedDocumentCount(MongoDatabase db, MongoCollection<RawBsonDocument> collection) {
		return collection.estimatedDocumentCount();
	}

	public static Number countDocuments(MongoDatabase db, MongoCollection<RawBsonDocument> collection) {
		return collection.countDocuments();
	}

	public Number getCollectionCount(MongoDatabase db, MongoCollection<RawBsonDocument> collection) {
		try {
			BsonDocument result = collection.aggregate(countPipeline).first();
			Number count = null;
			if (result != null) {
				count = result.get("count").asNumber().longValue();
			}
			return count;
		} catch (MongoCommandException mce) {
			logger.error(name + " getCollectionCount error");
			throw mce;
		}
	}

	public Number getFastCollectionCount(String dbName, String collName) {
		MongoDatabase db = mongoClient.getDatabase(dbName);
		return db.getCollection(collName).estimatedDocumentCount();
	}

	public Number getFastCollectionCount(MongoDatabase db, String collectionName) {
		return db.getCollection(collectionName).estimatedDocumentCount();
	}

	public Number getCollectionCount(String dbName, String collectionName) {
		MongoDatabase db = mongoClient.getDatabase(dbName);
		return getCollectionCount(db, db.getCollection(collectionName, RawBsonDocument.class));
	}

	public Number getCollectionCount(MongoDatabase db, String collectionName) {
		return getCollectionCount(db, db.getCollection(collectionName, RawBsonDocument.class));
	}

	public MongoCollection<Document> getShardsCollection() {
		return configDb.getCollection("shards");
	}

	public MongoCollection<Document> getTagsCollection() {
		return configDb.getCollection("tags");
	}

	public MongoCollection<Document> getChunksCollection() {
		return configDb.getCollection("chunks");
	}

	public MongoCollection<RawBsonDocument> getChunksCollectionRaw() {
		return configDb.getCollection("chunks", RawBsonDocument.class);
	}

	public MongoCollection<RawBsonDocument> getChunksCollectionRawPrivileged() {
		if (csrsMongoClient != null) {
			MongoDatabase configDb = csrsMongoClient.getDatabase("config");
			return configDb.getCollection("chunks", RawBsonDocument.class);
		} else {
			return getChunksCollectionRaw();
		}

	}

	public MongoCollection<Document> getDatabasesCollection() {
		return configDb.getCollection("databases");
	}

	public Document createDatabase(String databaseName) {
		logger.debug(name + " createDatabase " + databaseName);
		String tmpName = "tmp_ShardConfigSync_" + System.currentTimeMillis();
		mongoClient.getDatabase(databaseName).createCollection(tmpName);

		Document dbMeta = null;
		while (dbMeta == null) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			dbMeta = getDatabasesCollection().find(new Document("_id", databaseName)).first();
		}

		mongoClient.getDatabase(databaseName).getCollection(tmpName).drop();
		return dbMeta;
	}

//    public void createDatabaseOnShards(String databaseName) {
//    	logger.debug(String.format("%s - createDatabaseOnShards(): %s", name, databaseName));
//    	
//    	
//        String tmpName = "tmp_ShardConfigSync_" + System.currentTimeMillis();
//        mongoClient.getDatabase(databaseName).createCollection(tmpName);
//        
//        // ugly hack
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        mongoClient.getDatabase(databaseName).getCollection(tmpName).drop();
//    }

	public MongoIterable<String> listDatabaseNames() {
		return this.mongoClient.listDatabaseNames();
	}

	public Document listDatabases() {
		return this.mongoClient.getDatabase("admin").runCommand(listDatabasesCommand);
	}

	public Document dbStats(String dbName) {
		return this.mongoClient.getDatabase(dbName).runCommand(dbStatsCommand);
	}

	private Document collStatsCommand(String collName) {
		return new Document("collStats", collName);
	}

	public Document collStats(String dbName, String collName) {
		return this.mongoClient.getDatabase(dbName).runCommand(collStatsCommand(collName));
	}
	
	public void populateDatabaseCatalog() {
		databaseCatalogProvider.populateDatabaseCatalog();
	}

	public DatabaseCatalog getDatabaseCatalog()  {
		return getDatabaseCatalog(null);
	}

	public DatabaseCatalog getDatabaseCatalog(Set<Namespace> includeNs) {
		return databaseCatalogProvider.get(includeNs);
	}

	public ListCollectionsIterable<Document> listCollections(String dbName) {
		return mongoClient.getDatabase(dbName).listCollections();
	}

	public MongoIterable<String> listCollectionNames(String databaseName) {
		return this.mongoClient.getDatabase(databaseName).listCollectionNames();
	}

	public void flushRouterConfig() {

		Document flushRouterConfig = new Document("flushRouterConfig", true);

		try {
			logger.debug(String.format("flushRouterConfig for mongos"));
			mongoClient.getDatabase("admin").runCommand(flushRouterConfig);
		} catch (MongoTimeoutException timeout) {
			logger.debug("Timeout connecting", timeout);
		}

//		logger.debug(String.format("flushRouterConfig() for %s mongos routers", mongosMongoClients.size()));
//		for (Map.Entry<String, MongoClient> entry : mongosMongoClients.entrySet()) {
//			MongoClient client = entry.getValue();
//			Document flushRouterConfig = new Document("flushRouterConfig", true);
//
//			try {
//				logger.debug(String.format("flushRouterConfig for mongos %s", entry.getKey()));
//				client.getDatabase("admin").runCommand(flushRouterConfig);
//			} catch (MongoTimeoutException timeout) {
//				logger.debug("Timeout connecting", timeout);
//			}
//		}
	}

	public void stopBalancer() {
		if (versionArray.get(0) == 2 || (versionArray.get(0) == 3 && versionArray.get(1) <= 2)) {
			Document balancerId = new Document("_id", "balancer");
			Document setStopped = new Document("$set", new Document("stopped", true));
			UpdateOptions updateOptions = new UpdateOptions().upsert(true);
			configDb.getCollection("settings").updateOne(balancerId, setStopped, updateOptions);
		} else {
			adminCommand(new Document("balancerStop", 1));
		}
	}
	
	public Document runCommand(Document command, String dbName) {
		Document result = null;
		for (int i = 0; i < 2; i++) {
			try {
				result = mongoClient.getDatabase(dbName).runCommand(command);
				return result;
			} catch (MongoSocketException mse) {
				if (i == 0) {
					logger.warn("Socket exception in runCommand(), first attempt will retry", mse);
				} else {
					logger.error("Socket exception in runCommand(), last attempt, no more retries", mse);
				}
			}
		}
		
		return result;
	}

	public Document adminCommand(Document command) {
		return runCommand(command, "admin");
	}

	public String getVersion() {
		return version;
	}

	public List<Integer> getVersionArray() {
		return versionArray;
	}

	public boolean isVersion36OrLater() {
		if (versionArray.get(0) >= 4 || (versionArray.get(0) == 3 && versionArray.get(1) == 6)) {
			return true;
		}
		return false;
	}
	
	public boolean isVersion5OrLater() {
		if (versionArray.get(0) >= 5) {
			return true;
		}
		return false;
	}
	
	public boolean isVersion8OrLater() {
		if (versionArray.get(0) >= 8) {
			return true;
		}
		return false;
	}

	public boolean isAtlas() {
		if (connectionString != null) {
			List<String> hosts = connectionString.getHosts();
			if (hosts != null && hosts.size() > 0) {
				String host = hosts.get(0);
				return host.contains("mongodb.net");
			}
		}
		return false;
		
	}

	public Map<String, Shard> getShardsMap() {
		return shardsMap;
	}

	public Map<String, Document> getCollectionsMap() {
		return collectionsMap;
	}
	
	public BsonBinary getUuidForNamespace(String ns) {
		Document coll = collectionsMap.get(ns);
		if (coll == null) {
			this.populateCollectionsMap();
			coll = collectionsMap.get(ns);
			if (coll == null) {
				throw new IllegalArgumentException(String.format("Collection %s does not exist on %s", ns, name));
			}
		}
		
		UUID uuid = (UUID)coll.get("uuid");
		BsonBinary uuidBinary = BsonUuidUtil.uuidToBsonBinary(uuid);
		return uuidBinary;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public MongoCollection<Document> getCollection(String nsStr) {
		return getCollection(new Namespace(nsStr));
	}

	public MongoCollection<Document> getCollection(Namespace ns) {
		return mongoClient.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName());
	}

	public Document enableSharding(String dbName) {
		Document enableSharding = new Document("enableSharding", dbName);
		return adminCommand(enableSharding);
	}

	public Document shardCollection(Namespace ns, Document shardKey) {
		Document shardCommand = new Document("shardCollection", ns.getNamespace());
		shardCommand.append("key", shardKey);
		return adminCommand(shardCommand);
	}

	public MongoCollection<RawBsonDocument> getCollectionRaw(Namespace ns) {
		return mongoClient.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName(), RawBsonDocument.class);
	}

	public MongoCollection<BsonDocument> getCollectionBson(Namespace ns) {
		return mongoClient.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName(), BsonDocument.class);
	}

	public MongoDatabase getConfigDb() {
		return configDb;
	}

	public Collection<MongoClient> getMongosMongoClients() {
		return mongosMongoClients.values();
	}

	public MongoClient getShardMongoClient(String shardId) {
		// TODO clean this up
//    	if (!this.tertiaryShardsMap.isEmpty()) {
//    		String tid = tertiaryShardsMap.get(shardId).getId();
//    		return shardMongoClients.get(tid);
//    	} else {
//    		
//    	}
		return shardMongoClients.get(shardId);
	}

	public Map<String, MongoClient> getShardMongoClients() {
		return shardMongoClients;
	}
	
	public Set<String> getShardCollections(Namespace ns) {
		 Bson matchStage = match(eq("ns", ns.getNamespace()));
		 Bson groupStage = group(null, addToSet("shards", "$shard"));
		 Document result = configDb.getCollection("chunks").aggregate(asList(matchStage, groupStage)).first();
		 return new HashSet<>(result.getList("shards", String.class));
	}

	public void checkAutosplit() {
		logger.debug(String.format("checkAutosplit() for %s mongos routers", mongosMongoClients.size()));
		for (Map.Entry<String, MongoClient> entry : mongosMongoClients.entrySet()) {
			MongoClient client = entry.getValue();
			Document getCmdLine = new Document("getCmdLineOpts", true);
			Boolean autoSplit = null;
			try {
				// logger.debug(String.format("flushRouterConfig for mongos %s",
				// client.getAddress()));
				Document result = adminCommand(getCmdLine);
				Document parsed = (Document) result.get("parsed");
				Document sharding = (Document) parsed.get("sharding");
				if (sharding != null) {
					sharding.getBoolean("autoSplit");
				}
				if (autoSplit != null && !autoSplit) {
					logger.debug("autoSplit disabled for " + entry.getKey());
				} else {
					logger.warn("autoSplit NOT disabled for " + entry.getKey());
				}
			} catch (MongoTimeoutException timeout) {
				logger.debug("Timeout connecting", timeout);
			}
		}
	}

	public void disableAutosplit() {
		enableAutosplit(false);
	}

	public void enableAutosplit() {
		enableAutosplit(true);
	}
	
	private void enableAutosplit(boolean enabled) {
		MongoDatabase configDb = mongoClient.getDatabase("config");
		MongoCollection<RawBsonDocument> settings = configDb.getCollection("settings", RawBsonDocument.class);
		Document update = new Document("$set", new Document("enabled", enabled));
		settings.updateOne(eq("_id", "autosplit"), update);
	}


	public void extendTtls(String shardName, Namespace ns, Set<IndexSpec> sourceSpecs) {
		MongoClient client = getShardMongoClient(shardName);
		MongoDatabase db = client.getDatabase(ns.getDatabaseName());

		MongoCollection<Document> c = db.getCollection(ns.getCollectionName());
		// Document createIndexes = new Document("createIndexes",
		// ns.getCollectionName());
		List<Document> indexes = new ArrayList<>();
		// createIndexes.append("indexes", indexes);

		for (IndexSpec indexSpec : sourceSpecs) {

			Document indexInfo = indexSpec.getSourceSpec().decode(codec);
			// BsonDocument indexInfo = indexSpec.getSourceSpec().clone();
			indexInfo.remove("v");
			Number expireAfterSeconds = (Number) indexInfo.get("expireAfterSeconds");
			if (expireAfterSeconds != null) {
				logger.debug("ix: " + indexSpec);
				indexInfo.put("expireAfterSeconds", 50 * ShardConfigSync.SECONDS_IN_YEAR);
				logger.debug(String.format("Extending TTL for %s %s from %s to %s", ns, indexInfo.get("name"),
						expireAfterSeconds, indexInfo.get("expireAfterSeconds")));
				Document collMod = new Document("collMod", ns.getCollectionName());
				collMod.append("index", indexInfo);

				logger.debug(String.format("%s collMod: %s", ns, collMod));
				try {
					Document result = db.runCommand(collMod);
					logger.debug(String.format("%s collMod result: %s", ns, result));
				} catch (MongoCommandException mce) {
					logger.error(String.format("%s createIndexes failed: %s", ns, mce.getMessage()));
				}

			}
		}
	}

	public List<Role> getRoles() {
		MongoDatabase db = mongoClient.getDatabase("admin").withCodecRegistry(pojoCodecRegistry);
		MongoCollection<Role> rolesColl = db.getCollection("system.roles", Role.class);
		final List<Role> roles = new ArrayList<>();
		rolesColl.find().sort(Sorts.ascending("_id")).into(roles);
		
		for (Role role : roles) {
			
			Map<Resource, Set<String>> resourcePrivilegeActionsMap = new HashMap<>();
			
			List<Privilege> canonicalPriviliges = new ArrayList<>();
			
			for (Privilege p : role.getPrivileges()) {
				
				Set<String> canonicalActions = resourcePrivilegeActionsMap.get(p.getResource());
				if (canonicalActions == null) {
					canonicalActions = new HashSet<>();
					resourcePrivilegeActionsMap.put(p.getResource(), canonicalActions);
				}
				canonicalActions.addAll(p.getActions());
			}
			
			for (Map.Entry<Resource, Set<String>> entry : resourcePrivilegeActionsMap.entrySet()) {
				Privilege p2 = new Privilege();
				p2.setResource(entry.getKey());
				p2.setActions(new ArrayList<>(entry.getValue()));
				canonicalPriviliges.add(p2);
			}
			
			role.setPrivileges(canonicalPriviliges);
		}
		
		return roles;
	}

	public List<User> getUsers() {
		MongoDatabase db = mongoClient.getDatabase("admin").withCodecRegistry(pojoCodecRegistry);
		MongoCollection<User> usersColl = db.getCollection("system.users", User.class);
		final List<User> users = new ArrayList<>();
		usersColl.find().sort(Sorts.ascending("_id")).into(users);
		return users;
	}

	public void createIndexes(Namespace ns, Set<IndexSpec> sourceSpecs, boolean extendTtl, Document collation) {
		//MongoClient client = getShardMongoClient(shardName);
		MongoDatabase db = mongoClient.getDatabase(ns.getDatabaseName());

		MongoCollection<Document> c = db.getCollection(ns.getCollectionName());
		Document createIndexes = new Document("createIndexes", ns.getCollectionName());
		List<Document> indexes = new ArrayList<>();
		createIndexes.append("indexes", indexes);

		for (IndexSpec indexSpec : sourceSpecs) {
			logger.debug("ix: " + indexSpec);
			Document origIndexInfo = indexSpec.getSourceSpec().decode(codec);
			Document indexInfo = new Document();
			
			for (String key : origIndexInfo.keySet()) {
				if (IndexSpec.validIndexOptions.contains(key)) {
					indexInfo.append(key, origIndexInfo.get(key));
				} else {
					logger.warn("Ignoring invalid index option: {}", key);
				}
			}
			
			indexInfo.remove("v");
			indexInfo.remove("background");
			
			Number expireAfterSeconds = (Number) indexInfo.get("expireAfterSeconds");
			if (expireAfterSeconds != null && extendTtl) {

				indexInfo.put("expireAfterSeconds", 50 * ShardConfigSync.SECONDS_IN_YEAR);
				logger.debug(String.format("Extending TTL for %s %s from %s to %s", ns, indexInfo.get("name"),
						expireAfterSeconds, indexInfo.get("expireAfterSeconds")));

			}
			if (collation != null) {
				
				Document key = (Document)indexInfo.get("key");
				if (key != null && !key.containsKey("_id")) {
					indexInfo.put("collation", collation);
				}
				
			}
			indexes.add(indexInfo);
		}
		if (!indexes.isEmpty()) {

			try {
				Document createIndexesResult = db.runCommand(createIndexes);
				// logger.debug(String.format("%s result: %s", ns, createIndexesResult));
			} catch (MongoCommandException mce) {
				if (mce.getCode() == 85) {
					
				}
				logger.error(String.format("%s createIndexes failed: %s", ns, mce.getMessage()));
			}

		}
	}

	public void findOrphans(boolean doMove) {

		logger.debug("Starting findOrphans");

		MongoCollection<RawBsonDocument> sourceChunksColl = getChunksCollectionRaw();
		FindIterable<RawBsonDocument> sourceChunks = sourceChunksColl.find().noCursorTimeout(true)
				.sort(getChunkSort());

		String lastNs = null;
		int currentCount = 0;

		for (String shardId : shardsMap.keySet()) {

		}
		for (RawBsonDocument sourceChunk : sourceChunks) {
			String sourceNs = sourceChunk.getString("ns").getValue();

			if (!sourceNs.equals(lastNs)) {
				if (currentCount > 0) {
					logger.debug(String.format("findOrphans - %s - complete, queried %s chunks", lastNs, currentCount));
					currentCount = 0;
				}
				logger.debug(String.format("findOrphans - %s - starting", sourceNs));
			} else if (currentCount > 0 && currentCount % 10 == 0) {
				logger.debug(String.format("findOrphans - %s - currentCount: %s chunks", sourceNs, currentCount));
			}

			RawBsonDocument sourceMin = (RawBsonDocument) sourceChunk.get("min");
			RawBsonDocument sourceMax = (RawBsonDocument) sourceChunk.get("max");
			String sourceShard = sourceChunk.getString("shard").getValue();

			currentCount++;
			lastNs = sourceNs;
		}
	}

	public static String getNsFromChunk(BsonDocument doc) {
		BsonString bs = (BsonString) doc.get("ns");
		return bs.getValue();
	}

//	public String getIdFromChunk(RawBsonDocument sourceChunk) {
//		RawBsonDocument min = null;
//		BsonValue minVal = sourceChunk.get("min");
//		if (minVal instanceof BsonDocument) {
//			 min = (RawBsonDocument)minVal;
//		} else {
//			//TODO log this?
//		}
//		RawBsonDocument max = (RawBsonDocument) sourceChunk.get("max");
//		String ns = null;
//		if (sourceChunk.containsKey("ns")) {
//			ns = sourceChunk.getString("ns").getValue();
//		} else {
//			BsonBinary uuid = (BsonBinary)sourceChunk.get("uuid");
//			
//			UUID u = BsonUuidUtil.convertBsonBinaryToUuid(uuid);
//			ns = collectionsUuidMap.get(u);
//		}
//		
//		return getIdFromChunk(ns, min, max);
//	}
	
	public String getIdFromChunk(RawBsonDocument sourceChunk) {
		RawBsonDocument min = null;
		BsonValue minVal = sourceChunk.get("min");
		if (minVal instanceof BsonDocument) {
			 min = (RawBsonDocument)minVal;
		} else {
			throw new IllegalArgumentException("getIdFromChunk expected 'min' to be BsonDocument");
		}
		RawBsonDocument max = (RawBsonDocument) sourceChunk.get("max");
		
		String ns = null;
		if (sourceChunk.containsKey("ns")) {
			ns = sourceChunk.getString("ns").getValue();
		} else {
			BsonBinary bsonUuid = sourceChunk.getBinary("uuid");
			UUID uuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonUuid);
			this.collectionsUuidMap.get(uuid);
		}
		
		
		return getIdFromChunk(ns, min, max);
	}
	
	public static String getIdFromChunk(String ns, RawBsonDocument min, RawBsonDocument max) {
		String minHash = min.toJson();
		String maxHash = max.toJson();
		return String.format("%s_%s_%s", ns, minHash, maxHash);
	}

	public static String getShardFromChunk(BsonDocument chunk) {
		return chunk.getString("shard").getValue();
	}
	
	public Map<String, RawBsonDocument> getChunksCache(BsonDocument chunkQuery) {
		Map<String, RawBsonDocument> cache = new LinkedHashMap<>(); 
		return loadChunksCache(chunkQuery, cache);
	}
	
	public Bson getChunkSort() {
		if (this.isVersion5OrLater()) {
			return Sorts.ascending("uuid", "min");
		} else {
			return Sorts.ascending("ns", "min");
		}
		
	}
	
	/**
	 * Core method for loading chunks into a cache based on a query.
	 * This method is used by both full loading and namespace-specific reloading.
	 * 
	 * @param chunkQuery The query to filter chunks
	 * @param cache The cache to load chunks into
	 * @param clearExisting If true, clears the cache before loading
	 * @param namespaceFilter Optional namespace to filter existing entries when not clearing all
	 * @return The updated cache
	 */
	private Map<String, RawBsonDocument> loadChunksCacheInternal(BsonDocument chunkQuery, 
			Map<String, RawBsonDocument> cache, boolean clearExisting, String namespaceFilter) {
		
		// Clear existing entries if requested
		if (clearExisting) {
			if (namespaceFilter != null) {
				// Remove only entries for the specific namespace
				int removedCount = removeChunksForNamespace(cache, namespaceFilter);
				logger.debug("Removed {} existing chunks for namespace {} from cache", removedCount, namespaceFilter);
			} else {
				// Clear entire cache for full reload
				cache.clear();
				logger.debug("Cleared entire chunks cache");
			}
		}
		
		// Load new chunks
		MongoIterable<RawBsonDocument> sourceChunks = getSourceChunks(chunkQuery);
		int count = 0;
		for (Iterator<RawBsonDocument> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext();) {
			RawBsonDocument chunk = sourceChunksIterator.next();
			String chunkId = getIdFromChunk(chunk);
			cache.put(chunkId, chunk);
			count++;
		}
		
		String logMessage = namespaceFilter != null ? 
			String.format("*** %s: loaded %d chunks for namespace %s into chunksCache", name, count, namespaceFilter) :
			String.format("*** %s: loaded %d chunks into chunksCache", name, count);
		logger.debug(logMessage);
		
		return cache;
	}
	
	/**
	 * Removes chunks for a specific namespace from the cache.
	 * 
	 * @param cache The cache to remove chunks from
	 * @param namespace The namespace to remove chunks for
	 * @return The number of chunks removed
	 */
	private int removeChunksForNamespace(Map<String, RawBsonDocument> cache, String namespace) {
		Iterator<Map.Entry<String, RawBsonDocument>> iterator = cache.entrySet().iterator();
		int removedCount = 0;
		while (iterator.hasNext()) {
			Map.Entry<String, RawBsonDocument> entry = iterator.next();
			RawBsonDocument chunk = entry.getValue();
			String chunkNs = getNamespaceFromChunk(chunk);
			if (namespace.equals(chunkNs)) {
				iterator.remove();
				removedCount++;
			}
		}
		return removedCount;
	}
	
	public Map<String, RawBsonDocument> loadChunksCache(BsonDocument chunkQuery, Map<String, RawBsonDocument> cache) {
		// Full load - no namespace filter, don't clear existing (backwards compatibility)
		return loadChunksCacheInternal(chunkQuery, cache, false, null);
	}

	public Map<String, RawBsonDocument> loadChunksCache(BsonDocument chunkQuery) {
		return loadChunksCache(chunkQuery, chunksCache);
	}
	
	/**
	 * Reloads chunks for a specific namespace into the chunks cache.
	 * This method preserves the ordering of the existing cache by:
	 * 1. Removing all existing chunks for the namespace
	 * 2. Loading new chunks for the namespace
	 * 3. Merging them back while maintaining LinkedHashMap ordering
	 * 
	 * @param namespace The namespace to reload chunks for
	 * @return The updated chunks cache
	 */
	public Map<String, RawBsonDocument> reloadChunksCacheForNamespace(String namespace) {
		logger.debug("Reloading chunks cache for namespace: {}", namespace);
		
		// Create query for specific namespace
		BsonDocument namespaceQuery = createNamespaceChunkQuery(namespace);
		
		// Use the internal method with namespace-specific clearing
		return loadChunksCacheInternal(namespaceQuery, chunksCache, true, namespace);
	}
	
	/**
	 * Creates a chunk query for a specific namespace.
	 * Handles both version 5+ (UUID-based) and older (namespace-based) MongoDB versions.
	 * 
	 * @param namespace The namespace to create a query for
	 * @return A BsonDocument query for chunks of the specified namespace
	 */
	private BsonDocument createNamespaceChunkQuery(String namespace) {
		BsonDocument query = new BsonDocument();
		if (this.isVersion5OrLater()) {
			BsonBinary uuidBinary = getUuidForNamespace(namespace);
			if (uuidBinary != null) {
				query.append("uuid", new BsonDocument("$eq", uuidBinary));
			}
		} else {
			query.append("ns", new BsonDocument("$eq", new BsonString(namespace)));
		}
		return query;
	}
	
	/**
	 * Extracts the namespace from a chunk document.
	 * Handles both direct namespace field and UUID-based lookups for version 5+.
	 * 
	 * @param chunk The chunk document
	 * @return The namespace string, or null if not found
	 */
	private String getNamespaceFromChunk(RawBsonDocument chunk) {
		if (chunk.containsKey("ns")) {
			return chunk.getString("ns").getValue();
		} else if (chunk.containsKey("uuid")) {
			BsonBinary bsonUuid = chunk.getBinary("uuid");
			UUID uuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonUuid);
			return collectionsUuidMap.get(uuid);
		}
		return null;
	}
	
	private MongoIterable<RawBsonDocument> getSourceChunks(Bson chunkQuery) {
		if (chunkQuery == null) {
			throw new IllegalArgumentException("chunkQuery cannot be null");
		}
		
		MongoCollection<RawBsonDocument> chunksColl = getChunksCollectionRaw();

		MongoIterable<RawBsonDocument> sourceChunks;
		
		if (this.isVersion5OrLater()) {
			logger.debug("Building aggregation pipeline for version 5+");
			sourceChunks = chunksColl.aggregate(Arrays.asList(
		            match(chunkQuery),
		            lookup("collections", "uuid", "uuid", "collectionData"),
		            unwind("$collectionData"),
		            addFields(new Field<String>("ns", "$collectionData._id")),
		            project(fields(
		                excludeId(),
		                exclude("history", "lastmodEpoch", "lastmod", "collectionData")
		            )),
		            sort(getChunkSort())
		        ));
		} else {
			sourceChunks = chunksColl.find(chunkQuery)
					.projection(exclude("_id", "history", "lastmodEpoch", "lastmod"))
					.sort(getChunkSort());
		}
		return sourceChunks;
	}

	public Map<String, Map<String, RawBsonDocument>> loadChunksCacheMap(Document chunkQuery) {
		Map<String, Map<String, RawBsonDocument>> output = new HashMap<>();
		
		MongoIterable<RawBsonDocument> sourceChunks = getSourceChunks(chunkQuery);
		
		int count = 0;
		for (Iterator<RawBsonDocument> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext();) {
			RawBsonDocument chunk = sourceChunksIterator.next();
			String chunkId = getIdFromChunk(chunk);
			String shard = getShardFromChunk(chunk);

			if (!output.containsKey(shard)) {
				output.put(shard, new HashMap<>());
			}
			Map<String, RawBsonDocument> shardChunkCache = output.get(shard);
			shardChunkCache.put(chunkId, chunk);
			count++;
		}
		logger.debug("{}: loaded {} chunks into chunksCacheMap", name, count);
		return output;
	}

//	public boolean checkChunkExists(BsonDocument chunk) {
//		String id = getIdFromChunk(chunk);
//		return chunksCache.containsKey(id);
//	}

	public boolean checkChunkExists(BsonDocument chunk) {
		String ns = chunk.getString("ns").getValue();
		// if the dest chunk exists already, skip it
		
		Document query;
		if (this.isVersion5OrLater()) {
			// the uuids might be different between the chunk we are checking for and the target
			query = new Document("uuid", getUuidForNamespace(ns));
		} else {
			query = new Document("ns", ns);
		}
		 
		query.append("min", chunk.get("min"));
		query.append("max", chunk.get("max"));
		long count = getChunksCollectionRaw().countDocuments(query);
		return count > 0;
	}

	public void createChunk(BsonDocument chunk, boolean checkExists, boolean logErrors) {
		MongoCollection<RawBsonDocument> destChunksColl = getChunksCollectionRaw();
		String ns = chunk.getString("ns").getValue();

		if (checkExists) {
			boolean chunkExists = checkChunkExists(chunk);
			if (chunkExists) {
				return;
			}
		}

		BsonDocument max = (BsonDocument) chunk.get("max");

//		for (Iterator i = max.values().iterator(); i.hasNext();) {
//			Object next = i.next();
//			if (next instanceof MaxKey || next instanceof BsonMaxKey) {
//				return;
//			}
//		}


		Document splitCommand = new Document("split", ns);
		splitCommand.put("middle", max);
		// logger.debug("splitCommand: " + splitCommand);

		try {
			adminCommand(splitCommand);
		} catch (MongoCommandException mce) {
			if (logErrors) {
				logger.error("command error for namespace {}, message: {}", ns, mce.getMessage());
			}
		}

		if (checkExists) {
			boolean chunkExists = checkChunkExists(chunk);
			if (! chunkExists) {
				logger.warn("Chunk create failed: " + chunk);
			}
		}
	}
	
    public boolean splitChunk(String namespace, BsonDocument min, BsonDocument max, Document middle) {
        try {
            // Create the split command
            Document splitCmd = new Document("split", namespace)
                .append("middle", middle);
                //.append("bounds", Arrays.asList(min, max));
            
            // Execute the command
            Document result = adminCommand(splitCmd);
            
            // Check if the command was successful
            boolean success = false;
            Object successObj = result.get("ok");
            if (successObj instanceof Double) {
            	success = ((Double)successObj).equals(1.0);
            } else if (successObj instanceof Boolean) {
            	success = result.getBoolean("ok", false);
            } 
            if (!success) {
                logger.error("Failed to split chunk for namespace {}: {}", namespace, result);
            } else {
                logger.info("Successfully split chunk at {} for namespace {}", middle, namespace);
            }
            
            return success;
        } catch (Exception e) {
            logger.error("Error while splitting chunk for namespace {}: {}", namespace, e.getMessage(), e);
            return false;
        }
    }
	
	public Document splitFind(String ns, BsonDocument find, boolean logErrors) {
		Document splitCommand = new Document("split", ns);
		splitCommand.put("find", find);

		try {
			Document result = adminCommand(splitCommand);
			return result;
		} catch (MongoCommandException mce) {
			if (logErrors) {
				logger.error("command splitAt error for namespace {}, message: {}", ns, mce.getMessage());
			}
		}
		return null;
	}

	public Document splitAt(String ns, BsonDocument middle, boolean logErrors) {
		Document splitCommand = new Document("split", ns);
		splitCommand.put("middle", middle);

		try {
			Document result = adminCommand(splitCommand);
			return result;
		} catch (MongoCommandException mce) {
			if (logErrors) {
				logger.error("command splitAt error for namespace {}, message: {}", ns, mce.getMessage());
			}
		}
		return null;
	}
	
	public boolean moveChunk(BsonDocument chunk, String moveToShard, 
			boolean ignoreMissing, boolean secondaryThrottle, boolean waitForDelete) {
		BsonDocument min = (BsonDocument) chunk.get("min");
		BsonDocument max = (BsonDocument) chunk.get("max");
		String ns = chunk.getString("ns").getValue();
		return moveChunk(ns, min, max, moveToShard, ignoreMissing, secondaryThrottle, waitForDelete, false);
	}

	public boolean moveChunk(BsonDocument chunk, String moveToShard, boolean ignoreMissing) {
		return moveChunk(chunk, moveToShard, ignoreMissing, false, false);
	}
	
	public boolean moveChunk(String namespace, BsonDocument min, BsonDocument max, String moveToShard) {
		return moveChunk(namespace, min, max, moveToShard, false, false, false, false, false);
	}
	
	public boolean moveChunk(String namespace, BsonDocument min, BsonDocument max, String moveToShard, 
			boolean ignoreMissing, boolean secondaryThrottle, boolean waitForDelete, boolean majorityWrite) {
		return moveChunk(namespace, min, max, moveToShard, ignoreMissing, secondaryThrottle, waitForDelete, majorityWrite, false);
	}

	public boolean moveChunk(String namespace, BsonDocument min, BsonDocument max, String moveToShard, 
			boolean ignoreMissing, boolean secondaryThrottle, boolean waitForDelete, boolean majorityWrite, boolean throwCommandExceptions) {
		Document moveChunkCmd = new Document("moveChunk", namespace);
		moveChunkCmd.append("bounds", Arrays.asList(min, max));
		moveChunkCmd.append("to", moveToShard);
		//if (version.startsWith("4.4")) {
		//	moveChunkCmd.append("forceJumbo", true);
		//}
		if (secondaryThrottle) {
			moveChunkCmd.append("_secondaryThrottle", secondaryThrottle);
		}
		if (majorityWrite) {
			moveChunkCmd.append("writeConcern", WriteConcern.MAJORITY.asDocument());
		}
		if (waitForDelete) {
			moveChunkCmd.append("_waitForDelete", waitForDelete);
		}
		
		try {
			adminCommand(moveChunkCmd);
		} catch (MongoCommandException mce) {
			if (!ignoreMissing) {
				logger.warn(String.format("moveChunk error ns: %s, message: %s", namespace, mce.getMessage()));
			}
			if (throwCommandExceptions) {
				throw mce;
			}
			return false;
		}
		return true;
	}
	
	/**
	 * Merges contiguous chunks in a sharded collection.
	 * 
	 * @param namespace The namespace (database.collection) to merge chunks in
	 * @param min The lower bound of the chunk range to merge
	 * @param max The upper bound of the chunk range to merge
	 * @return The result document from the mergeChunks command, or null if an error occurred
	 */
	public Document mergeChunks(String namespace, BsonDocument min, BsonDocument max) {
		return mergeChunks(namespace, min, max, true);
	}
	
	/**
	 * Merges contiguous chunks in a sharded collection.
	 * 
	 * @param namespace The namespace (database.collection) to merge chunks in
	 * @param min The lower bound of the chunk range to merge
	 * @param max The upper bound of the chunk range to merge
	 * @param logErrors Whether to log errors that occur during the merge
	 * @return The result document from the mergeChunks command, or null if an error occurred
	 */
	public Document mergeChunks(String namespace, BsonDocument min, BsonDocument max, boolean logErrors) {
		Document mergeChunksCmd = new Document("mergeChunks", namespace);
		mergeChunksCmd.append("bounds", Arrays.asList(min, max));
		
		try {
			Document result = adminCommand(mergeChunksCmd);
			return result;
		} catch (MongoCommandException mce) {
			if (logErrors) {
				logger.error("mergeChunks error for namespace {}, message: {}", namespace, mce.getMessage());
			}
			return null;
		}
	}
	
	/**
	 * Merges chunks based on a chunk document.
	 * 
	 * @param chunk The chunk document containing namespace, min and max bounds
	 * @return The result document from the mergeChunks command, or null if an error occurred
	 */
	public Document mergeChunks(BsonDocument chunk) {
		return mergeChunks(chunk, true);
	}
	
	/**
	 * Merges chunks based on a chunk document.
	 * 
	 * @param chunk The chunk document containing namespace, min and max bounds
	 * @param logErrors Whether to log errors that occur during the merge
	 * @return The result document from the mergeChunks command, or null if an error occurred
	 */
	public Document mergeChunks(BsonDocument chunk, boolean logErrors) {
		BsonDocument min = (BsonDocument) chunk.get("min");
		BsonDocument max = (BsonDocument) chunk.get("max");
		String ns = chunk.getString("ns").getValue();
		return mergeChunks(ns, min, max, logErrors);
	}
	
	public List<Document> splitVector(Namespace ns, Document collectionMeta) {
		Document splitVectorCmd = new Document("splitVector", ns.getNamespace());
		Document keyPattern = (Document)collectionMeta.get("key");
		splitVectorCmd.append("keyPattern", keyPattern);
		splitVectorCmd.append("maxChunkSizeBytes", ONE_GIGABYTE);
		MongoDatabase dbTop = mongoClient.getDatabase(ns.getDatabaseName());

		Document stats = dbTop.runCommand(new Document("collStats", ns.getCollectionName()));
		Long size = ((Number) stats.get("size")).longValue();

		int shardCount = this.shardsMap.size();
		long splitSize = (size / shardCount) / 10;

		if (splitSize >= 16793599) {
			splitSize = 16793599;
		} else if (splitSize < ONE_MEGABYTE) {
			splitSize = 2 * ONE_MEGABYTE;
		}

		logger.debug("{}: size: {}, splitSize: {}", ns, size, splitSize);
		splitVectorCmd.append("maxChunkSizeBytes", splitSize);

		Document splits = null;
		List<Document> splitKeys = null;
		List<Document> splitKeysAll = new ArrayList<>();
		for (Map.Entry<String, MongoClient> entry : shardMongoClients.entrySet()) {
			MongoClient mongoClient = entry.getValue();
			MongoDatabase db = mongoClient.getDatabase(ns.getDatabaseName());
			String shardId = entry.getKey();
			Integer splitCount = null;
			try {
				splits = db.runCommand(splitVectorCmd);
				splitKeys = (List<Document>) splits.get("splitKeys");
				splitKeysAll.addAll(splitKeys);
				splitCount = splitKeys.size();
			} catch (MongoCommandException mce) {
				if (mce.getCode() == 13) {
					logger.error("splitVector failed: {}, note this cannot be run on an Atlas source", mce.getMessage());
				} else if (mce.getMessage().contains("couldn't find index over splitting key")) {
					long count = db.getCollection(ns.getCollectionName()).estimatedDocumentCount();
					logger.warn("shard {} splitVector failed for ns {}, index {} does not exist. estimated doc count: {}", shardId, ns.getNamespace(), keyPattern, count);
					db.getCollection(ns.getCollectionName()).createIndex(keyPattern, new IndexOptions().background(true));
				} else if (mce.getMessage().contains("ns not found")) {
					// ignore
				} else {
					logger.error("splitVector unexpected error for ns {}, message: {}", ns, mce.getMessage());
				}
			}
			logger.debug("{}: shard: {}, ns: {}, key: {}, splitCount: {}", name, shardId, ns.getNamespace(), keyPattern, splitCount);
		}
		return splitKeysAll;
	}

	public Map<Namespace, List<Document>> splitVector() {

		Map<Namespace, List<Document>> splitPoints = new TreeMap<>();

		for (Document sourceColl : getCollectionsMap().values()) {

			String nsStr = (String) sourceColl.get("_id");
			Namespace ns = new Namespace(nsStr);
			if (excludedSystemDbs.contains(ns.getDatabaseName())) {
				continue;
			}

			Document splitVectorCmd = new Document("splitVector", nsStr);
			Document keyPattern = (Document)sourceColl.get("key");
			splitVectorCmd.append("keyPattern", keyPattern);
			splitVectorCmd.append("maxChunkSizeBytes", ONE_GIGABYTE);

			Document splits = null;
			List<Document> splitKeys = null;
			List<Document> splitKeysAll = new ArrayList<>();
			for (Map.Entry<String, MongoClient> entry : shardMongoClients.entrySet()) {
				MongoClient mongoClient = entry.getValue();
				String shardId = entry.getKey();
				MongoDatabase db = mongoClient.getDatabase(ns.getDatabaseName());

				Integer splitCount = null;
				try {
					splits = db.runCommand(splitVectorCmd);
					splitKeys = (List<Document>) splits.get("splitKeys");
					splitKeysAll.addAll(splitKeys);
					splitCount = splitKeys.size();
				} catch (MongoCommandException mce) {
					if (mce.getCode() == 13) {
						logger.error("splitVector failed: {}, note this cannot be run on an Atlas source", mce.getMessage());
					} else if (mce.getMessage().contains("couldn't find index over splitting key")) {
						long count = db.getCollection(ns.getCollectionName()).estimatedDocumentCount();
						logger.warn("shard {} splitVector failed for ns {}, index {} does not exist. estimated doc count: {}", shardId, nsStr, keyPattern, count);
						db.getCollection(ns.getCollectionName()).createIndex(keyPattern, new IndexOptions().background(true));
//						List<Document> indexes = new ArrayList<>();
//						db.getCollection(ns.getCollectionName()).listIndexes().into(indexes);
//						for (Document index : indexes) {
//							logger.warn("    {}", index);
//						}

					} else {
						logger.error("splitVector unexpected error", mce);
					}
				}
				logger.debug("{}: shard: {}, ns: {}, key: {}, splitCount: {}", name, shardId, nsStr, keyPattern, splitCount);
			}
			splitPoints.put(ns, splitKeysAll);

		}
		return splitPoints;
	}


	public ConnectionString getConnectionString() {
		return connectionString;
	}

	public String getName() {
		return name;
	}

	public String getRsPattern() {
		return rsPattern;
	}

	public void setRsPattern(String rsPattern) {
		this.rsPattern = rsPattern;
	}

	public Map<String, Shard> getTertiaryShardsMap() {
		return tertiaryShardsMap;
	}

	public void setCsrsUri(String csrsUri) {
		this.csrsUri = csrsUri;
	}

	public boolean isMongos() {
		return mongos;
	}

	public ShardClientType getShardClientType() {
		return shardClientType;
	}

	public void setShardClientType(ShardClientType shardClientType) {
		this.shardClientType = shardClientType;
	}

	public String[] getRsStringsManual() {
		return rsStringsManual;
	}

	public void setRsStringsManual(String[] rsStringsManual) {
		this.rsStringsManual = rsStringsManual;
	}

	public String getRsRegex() {
		return rsRegex;
	}

	public void setRsRegex(String rsRegex) {
		this.rsRegex = rsRegex;
	}

	public void setRsSsl(Boolean rsSsl) {
		this.rsSsl = rsSsl;
	}

	public Map<UUID, String> getCollectionsUuidMap() {
		return collectionsUuidMap;
	}

}
