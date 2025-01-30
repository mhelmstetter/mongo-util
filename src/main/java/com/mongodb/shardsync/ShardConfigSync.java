package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.SetUtils;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.UuidCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.atlas.AtlasServiceGenerator;
import com.mongodb.atlas.AtlasUtil;
import com.mongodb.atlas.model.AtlasRole;
import com.mongodb.atlas.model.AtlasRoleResponse;
import com.mongodb.atlas.model.AtlasUser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.model.IndexSpec;
import com.mongodb.model.Namespace;
import com.mongodb.model.Privilege;
import com.mongodb.model.Role;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.model.User;
import com.mongodb.mongomirror.MongoMirrorRunner;
import com.mongodb.mongomirror.model.MongoMirrorStatus;
import com.mongodb.mongomirror.model.MongoMirrorStatusInitialSync;
import com.mongodb.mongomirror.model.MongoMirrorStatusOplogSync;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import picocli.CommandLine.Command;

@Command(name = "shardSync", mixinStandardHelpOptions = true, version = "shardSync 1.0")
public class ShardConfigSync implements Callable<Integer> {


    private final static DocumentCodec codec = new DocumentCodec();

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm_ss");

    private static Logger logger = LoggerFactory.getLogger(ShardConfigSync.class);

    private final static int BATCH_SIZE = 512;

    public final static int SECONDS_IN_YEAR = 31536000;

    private final static Document LOCALE_SIMPLE = new Document("locale", "simple");

    private ShardClient sourceShardClient;
    private ShardClient destShardClient;

    private ChunkManager chunkManager;
    private AtlasUtil atlasUtil;

    private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();
    private Map<String, Document> destDbInfoMap = new TreeMap<String, Document>();

    private SyncConfiguration config;

    CodecRegistry registry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(new UuidCodecProvider(UuidRepresentation.STANDARD),
                    PojoCodecProvider.builder().automatic(true).build()));

    DocumentCodec documentCodec = new DocumentCodec(registry);

    public ShardConfigSync(SyncConfiguration config) {
        Package pkg = ShardConfigSync.class.getPackage();
        String version = pkg.getImplementationVersion();
        logger.debug("ShardConfigSync starting - mongo-util version {}", version);
        this.config = config;
    }

    @Override
    public Integer call() throws Exception {
        return 0;
    }

    public void initialize() {
        this.sourceShardClient = config.getSourceShardClient();
        this.destShardClient = config.getDestShardClient();
        initAtlasUtil();
    }

    public void initChunkManager() {
        if (chunkManager == null) {
            chunkManager = new ChunkManager(config);
            chunkManager.initalize();
            this.sourceShardClient = config.getSourceShardClient();
            this.destShardClient = config.getDestShardClient();
        }

    }

    public void initAtlasUtil() {
        if (config.atlasApiPublicKey == null) {
            return;
        }
        try {
            atlasUtil = new AtlasUtil(config.atlasApiPublicKey, config.atlasApiPrivateKey);
        } catch (KeyManagementException | NoSuchAlgorithmException e1) {
            logger.error("error initializing AtlasUtil", e1);
        }
    }

    public void shardCollections() {
        logger.debug("Starting shardCollections");
        sourceShardClient.populateCollectionsMap();
        enableDestinationSharding();
        shardDestinationCollections();
    }

    public void flushRouterConfig() {
        destShardClient.flushRouterConfig();
    }

    private void checkDestShardClientIsMongos() {
        if (config.getDestRsPattern() != null) {
            return;
        }
        if (!destShardClient.isMongos() && !config.isShardToRs()) {
            throw new IllegalArgumentException("dest connection must be to a mongos router unless using shardToRs");
        }
    }
    
    private void createCollections() {
    	createCollections(null);
    }

    private void createCollections(SyncConfiguration syncConfig) {
        MongoClient sourceClient = sourceShardClient.getMongoClient();
        MongoClient destClient = destShardClient.getMongoClient();
        
        destShardClient.populateCollectionsMap();
        Map<String, Document> existingDestCollections = destShardClient.getCollectionsMap();

        for (String dbName : sourceClient.listDatabaseNames()) {
            MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
            for (Document collectionInfo : sourceDb.listCollections()) {
                String collectionName = collectionInfo.getString("name");
                Namespace ns = new Namespace(dbName, collectionName);

                if (config.filterCheck(ns)) {
                    continue;
                }
                String type = collectionInfo.getString("type");
                if (collectionName.equals("system.views") || (type != null && type.equals("view"))) {
                    logger.warn("Skipping view: {}", ns);
                    continue;
                }

                if (existingDestCollections.containsKey(ns.getNamespace())) {
                	logger.debug("ns {} exists, won't create", ns);
                    continue;
                }
                
                if (syncConfig != null && syncConfig.getWiredTigerConfigString() != null) {
                	Document options;
                    if (collectionInfo.containsKey("options")) {
                    	options = (Document)collectionInfo.get("options");
                    } else {
                    	options = new Document();
                    	collectionInfo.put("options", options);
                    }
                    
                    Document storageEngine;
                	if (options.containsKey("storageEngine")) {
                		storageEngine = (Document)options.get("storageEngine");
                	} else {
                		storageEngine = new Document();
                		options.put("storageEngine", storageEngine);
                	}
                	
                	Document wiredTiger;
                	if (storageEngine.containsKey("wiredTiger")) {
                		wiredTiger = (Document)options.get("wiredTiger");
                	} else {
                		wiredTiger = new Document();
                		storageEngine.put("wiredTiger", wiredTiger);
                	}
                	
                	String configString;
                	if (wiredTiger.containsKey("configString")) {
                		configString = wiredTiger.getString("configString");
                		if (configString != null && configString.length() > 0) {
                			if (! configString.contains("block_compressor")) {
                    			configString = configString + "," + syncConfig.getWiredTigerConfigString();
                    		}
                		} else {
                			configString = syncConfig.getWiredTigerConfigString();
                		}
                		
                	} else {
                		configString = syncConfig.getWiredTigerConfigString();
                		wiredTiger.put("configString", configString);
                	}
                }
                
                

                try {
                    destClient.getDatabase(dbName).createCollection(collectionName, getCreateCollectionOptions(collectionInfo));
                    logger.debug("created collection {}.{}", dbName, collectionName);
                } catch (MongoException me) {
                    logger.error("createCollection failed, confirm that target is clean/empty", me);
                    throw me; // fatal
                }

            }
        }
    }

    private CreateCollectionOptions getCreateCollectionOptions(Document collectionInfo) {
        CreateCollectionOptions opts = new CreateCollectionOptions();
        Document options = collectionInfo.get("options", Document.class);

        if (options.isEmpty()) {
            return opts;
        }
        logger.info("non default collection options: {}", collectionInfo);

        Document collationDoc = options.get("collation", Document.class);
        if (collationDoc != null) {
            Collation collation = getCollation(collationDoc);
            opts.collation(collation);
        }
        
        Document storageEngine = options.get("storageEngine", Document.class);
        if (storageEngine != null) {
        	opts.storageEngineOptions(storageEngine);
        }
        
        Document validator = options.get("validator", Document.class);
        if (validator != null) {
        	ValidationOptions validationOpts = new ValidationOptions().validator(validator);
        	opts.validationOptions(validationOpts);
        }

        Boolean capped = options.getBoolean("capped");
        if (capped != null && capped) {
            opts.capped(capped);
            Object max = options.get("max");
            if (max != null && max instanceof Number) {
                Number maxNum = (Number) max;
                opts.maxDocuments(maxNum.longValue());
            } else if (max != null) {
                logger.error("Unexpected type for max: {}, value: {}", max.getClass().getName(), max);
            }
            Object size = options.get("size");
            if (size != null && size instanceof Number) {
                Number sizeNum = (Number) size;
                opts.sizeInBytes(sizeNum.longValue());
            } else if (size != null) {
                logger.error("Unexpected type for size: {}, value: {}", size.getClass().getName(), size);
            }

        }

        return opts;
    }

    private Collation getCollation(Document collation) {
        Collation.Builder builder = Collation.builder();
        builder.locale(collation.getString("locale"));
        builder.caseLevel(collation.getBoolean("caseLevel"));
        builder.collationCaseFirst(CollationCaseFirst.fromString(collation.getString("caseFirst")));
        builder.collationStrength(CollationStrength.fromInt(collation.getInteger("strength")));
        builder.numericOrdering(collation.getBoolean("numericOrdering"));
        builder.collationAlternate(CollationAlternate.fromString(collation.getString("alternate")));
        builder.collationMaxVariable(CollationMaxVariable.fromString(collation.getString("maxVariable")));
        builder.normalization(collation.getBoolean("normalization"));
        builder.backwards(collation.getBoolean("backwards"));

        return builder.build();
    }

    private Map<Namespace, Set<IndexSpec>> getIndexSpecs(MongoClient client, Set<String> filterSet) {
        Map<Namespace, Set<IndexSpec>> sourceIndexSpecs = new LinkedHashMap<>();
        for (String dbName : client.listDatabaseNames()) {
            MongoDatabase sourceDb = client.getDatabase(dbName);
            for (Document collectionInfo : sourceDb.listCollections()) {
                String collectionName = collectionInfo.getString("name");
                String type = collectionInfo.getString("type");
                Namespace ns = new Namespace(dbName, collectionName);
                if (config.filterCheck(ns) || (filterSet != null && !filterSet.contains(ns.getNamespace()))) {
                    continue;
                }

                if (collectionName.equals("system.views") || (type != null && type.equals("view"))) {
                    logger.debug("Skipping view: {}", ns);
                    continue;
                }

                MongoCollection<RawBsonDocument> collection = sourceDb.getCollection(collectionName, RawBsonDocument.class);
                Set<IndexSpec> indexSpecs = getCollectionIndexSpecs(collection);
                sourceIndexSpecs.put(ns, indexSpecs);

            }
        }
        return sourceIndexSpecs;
    }

    private Set<IndexSpec> getCollectionIndexSpecs(MongoCollection<RawBsonDocument> collection) {
        Set<IndexSpec> indexSpecs = new HashSet<>();
        Namespace ns = new Namespace(collection.getNamespace());
        for (RawBsonDocument sourceSpec : collection.listIndexes(RawBsonDocument.class)) {
            IndexSpec spec = null;
            try {
                spec = IndexSpec.fromDocument(sourceSpec, ns);
                indexSpecs.add(spec);
            } catch (BSONException be) {
                logger.error("Error getting index spec: {}", sourceSpec);
                logger.error("error", be);
            }
        }
        return indexSpecs;
    }

    public void syncIndexesShards(boolean createMissing, boolean extendTtl, String collationStr) {
        logger.debug(String.format("Starting syncIndexes: extendTtl: %s", extendTtl));

        Document collation = null;
        if (collationStr != null) {
            collation = Document.parse(collationStr);
        }


        //sourceShardClient.populateShardMongoClients();
        Map<Namespace, Set<IndexSpec>> sourceIndexSpecs = getIndexSpecs(sourceShardClient.getMongoClient(), null);

        //Map<Namespace, Set<IndexSpec>> destShardIndexSpecs = getIndexSpecs(destShardClient.getMongoClient(), null);

        for (Map.Entry<Namespace, Set<IndexSpec>> sourceEntry : sourceIndexSpecs.entrySet()) {
            Namespace ns = sourceEntry.getKey();
            Set<IndexSpec> sourceSpecs = sourceEntry.getValue();

            //Set<IndexSpec> destSpecs = destShardIndexSpecs.get(ns);

            if (createMissing) {
                //logger.debug(String.format("%s - missing dest indexes %s missing, creating", ns, diff));
                destShardClient.createIndexes(ns, sourceSpecs, extendTtl, collation);
            }
        }
    }

    public void compareIndexes(boolean collModTtl) {
        logger.debug("Starting compareIndexes");
        Map<Namespace, Set<IndexSpec>> sourceIndexSpecs = getIndexSpecs(sourceShardClient.getMongoClient(), null);
        Map<Namespace, Set<IndexSpec>> destIndexSpecs = getIndexSpecs(destShardClient.getMongoClient(), null);
        int diffCount = 0;
        int indexCount = 0;
        int modifiedCount = 0;
        //MapDifference<Namespace, Set<IndexSpec>> diff = Maps.difference(sourceIndexSpecs, destIndexSpecs);

        for (Map.Entry<Namespace, Set<IndexSpec>> entry : sourceIndexSpecs.entrySet()) {
            Namespace ns = entry.getKey();
            Set<IndexSpec> sourceSpecs = entry.getValue();
            indexCount += sourceSpecs.size();
            Set<IndexSpec> destSpecs = destIndexSpecs.get(ns);
            if (destSpecs == null || destSpecs.isEmpty()) {
                logger.warn("Destination indexes not found for ns: {}", ns);
                continue;
            }
            Set<IndexSpec> diff = Sets.difference(sourceSpecs, destSpecs);

            if (!diff.isEmpty()) {
                logger.debug("Indexes differ for ns: {}, diff: {}", ns, diff);
                diffCount += diff.size();
                if (collModTtl) {
                    modifiedCount += collModTtl(sourceIndexSpecs, diff);
                }
            } else if (config.extendTtl) {
                logger.debug("collModTtl with extendTtl");
                modifiedCount += collModTtl(sourceIndexSpecs, sourceSpecs);
            }
        }
        if (collModTtl) {
            logger.debug("collModTtl {} indexes modified", modifiedCount);
        } else {
            logger.debug("Checked {} indexes, {} indexes failed", indexCount, diffCount);
        }

    }

    public void checkShardedIndexes() {
        logger.debug("Starting checkShardedIndexes");
        destShardClient.populateShardMongoClients();
        destShardClient.populateCollectionsMap();
        Map<String, Document> collectionsMap = destShardClient.getCollectionsMap();
        destShardClient.getShardMongoClients();

        for (Map.Entry<String, Document> entry : collectionsMap.entrySet()) {
            Document collSpec = entry.getValue();
            String nsStr = (String) collSpec.get("_id");
            Namespace ns = new Namespace(nsStr);

            Set<String> shards = destShardClient.getShardCollections(ns);

            Set<IndexSpec> lastShardIndexSpecs = null;
            String lastShard = null;
            for (Map.Entry<String, MongoClient> mce : destShardClient.getShardMongoClients().entrySet()) {
                MongoClient mc = mce.getValue();
                String shard = mce.getKey();

                if (!shards.contains(shard)) {
                    continue;
                }


                Set<IndexSpec> indexSpecs = getCollectionIndexSpecs(mc.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName(), RawBsonDocument.class));

                //System.out.println(mce.getKey() + " " + ns + " " + indexSpecs.size());

                if (lastShardIndexSpecs != null) {

                    Set<IndexSpec> diff1 = SetUtils.disjunction(lastShardIndexSpecs, indexSpecs);
                    if (!diff1.isEmpty()) {
                        logger.debug("Indexes differ for ({} / {}) ns: {}, diff: {}", lastShard, shard, ns, diff1);
                    }
                }

                lastShard = shard;
                lastShardIndexSpecs = indexSpecs;
            }

        }
    }

    private int collModTtl(Map<Namespace, Set<IndexSpec>> sourceIndexSpecsMap, Set<IndexSpec> diff) {
        int modifiedCount = 0;
        for (IndexSpec spec : diff) {
            if (spec.getExpireAfterSeconds() != null) {
                Namespace ns = spec.getNamespace();

                Document indexInfo = spec.getSourceSpec().decode(codec);
                indexInfo.remove("v");
                Document collMod = new Document("collMod", ns.getCollectionName());
                collMod.append("index", indexInfo);

                if (config.extendTtl) {
                    Number expireAfterSeconds = (Number) indexInfo.get("expireAfterSeconds");
                    indexInfo.put("expireAfterSeconds", 50 * ShardConfigSync.SECONDS_IN_YEAR);
                    logger.debug(String.format("Extending TTL for %s %s from %s to %s", ns, indexInfo.get("name"),
                            expireAfterSeconds, indexInfo.get("expireAfterSeconds")));
                }

                logger.debug(String.format("%s collMod: %s", ns, collMod));
                try {
                    Document result = destShardClient.runCommand(collMod, ns.getDatabaseName());
                    logger.debug(String.format("%s collMod result: %s", ns, result));
                    modifiedCount++;
                } catch (MongoCommandException mce) {
                    logger.error(String.format("%s createIndexes failed: %s", ns, mce.getMessage()));
                }
            }
        }
        return modifiedCount;
    }

    public void diffRoles() {

        logger.debug("Starting diffRoles");
        List<Role> sourceRoles = this.sourceShardClient.getRoles();
        Map<String, Role> sourceRolesMap = sourceRoles.stream().collect(Collectors.toMap(Role::getId, Function.identity()));

        List<Role> destRoles = this.destShardClient.getRoles();
        Map<String, Role> destRolesMap = destRoles.stream().collect(Collectors.toMap(Role::getId, Function.identity()));

        for (Map.Entry<String, Role> entry : sourceRolesMap.entrySet()) {

            Role sourceRole = entry.getValue();
            Role destRole = destRolesMap.get(entry.getKey());

            if (destRole == null) {
                continue;
            }

//			logger.debug("*** sourcePrivileges {} -  ***", sourceRole.getId());
//			for (Privilege p : sourceRole.getPrivileges()) {
//				if (! UsersRolesManager.ignoredCollections.contains(p.getResource().getCollection())) {
//					logger.debug(p.toString());
//				}
//				
//			}
//			
//			logger.debug("*** destPrivileges {} -  ***", sourceRole.getId());
//			for (Privilege p : destRole.getPrivileges()) {
//				if (! UsersRolesManager.ignoredCollections.contains(p.getResource().getCollection())) {
//					logger.debug(p.toString());
//				}
//				
//			}


            Set<Privilege> sourceRoleMap = sourceRole.getResoucePrivilegeSet();
            Set<Privilege> destRoleMap = destRole.getResoucePrivilegeSet();

            Sets.SetView<Privilege> diff = Sets.difference(sourceRoleMap, destRoleMap);

            for (Iterator<Privilege> it = diff.iterator(); it.hasNext(); ) {
                Privilege p = it.next();
                if (!UsersRolesManager.ignoredCollections.contains(p.getResource().getCollection())) {
                    logger.debug("onlyOnSource: {}", p);
                }

            }
        }
    }

    public void diffUsers() {

    }

    public void testUsersAuth() throws IOException {
        Map<String, String> usersMap = readUsersInputCsv();

        ConnectionString cs = destShardClient.getConnectionString();

        for (Map.Entry<String, String> entry : usersMap.entrySet()) {
            String username = entry.getKey();
            String password = entry.getValue();

            MongoCredential credential = MongoCredential.createScramSha1Credential(username, "admin", password.toCharArray());

            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(cs)
                    .credential(credential)
                    .build();
            MongoClient mongoClient = MongoClients.create(mongoClientSettings);
            try {
                mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
                logger.debug("{} auth pass", username);
            } catch (MongoSecurityException mse) {
                logger.warn("{} auth fail", username);
            } catch (Exception e) {
                logger.error("{} auth fail - Invalid password? - {}", username, e.getMessage());
            }

        }
    }

    public void syncRoles() throws IOException {

        List<Role> roles = this.sourceShardClient.getRoles();
        List<AtlasRole> atlasRoles = UsersRolesManager.convertMongoRolesToAtlasRoles(roles);
        Set<String> roleNames = new HashSet<>();

        for (AtlasRole role : atlasRoles) {
            try {
                if (role.getActions().isEmpty() && role.getInheritedRoles().isEmpty()) {
                    logger.warn("ignoring role {}, no actions or inherited roles", role.getRoleName());
                    continue;
                }

                AtlasRoleResponse result = atlasUtil.createCustomDbRole(config.atlasProjectId, role);
                if (result.isSuccess()) {
                    logger.debug("Custom db role {} created", role.getRoleName());
                    roleNames.add(role.getRoleName());
                } else if (result.isDuplicate()) {
                    logger.debug("Custom db role {} already exists", role.getRoleName());
                } else {
                    logger.error("Custom db role {} failed: {}", role.getRoleName(), result.getResponseError());
                    ObjectMapper mapper = new ObjectMapper();
                    String jsonInString = mapper.writeValueAsString(role);
                    logger.error("failed role json: {}", jsonInString);
                }

            } catch (IOException | KeyManagementException | NoSuchAlgorithmException e) {
                logger.error("Error creating custom db role: {}", role.getRoleName(), e);
            }
        }

    }

    private boolean destUserExists(User user) {
        Document userDoc = new Document("user", user.getUser()).append("db", user.getDb());
        Document userInfoCmd = new Document("usersInfo", userDoc);
        Document usersInfoResult = this.destShardClient.adminCommand(userInfoCmd);
        List<Document> users = usersInfoResult.getList("users", Document.class);
        return users != null && !users.isEmpty();
    }

    private Map<String, String> readUsersInputCsv() throws IOException {
        Map<String, String> usersMap = null;
        String usersInputCsv = config.getUsersInputCsv();
        File usersInputFile = null;
        if (usersInputCsv != null) {
            usersInputFile = new File(usersInputCsv);
            if (usersInputFile.exists()) {
                CSVReader csvReader = new CSVReaderBuilder(new FileReader(usersInputFile))
                        .withSkipLines(1)
                        //.withCSVParser(parser)
                        .build();
                usersMap = new LinkedHashMap<>();
                List<String[]> lines = csvReader.readAll();
                for (String[] line : lines) {
                    usersMap.put(line[0], line[1]);
                }
                logger.debug("readUsersInputCsv() populated {} users from {}", usersMap.size(), usersInputCsv);
            }
        }
        return usersMap;
    }

    public void syncUsers() throws IOException {

        boolean sourceIsAtlas = this.sourceShardClient.isAtlas();
        boolean destIsAtlas = this.destShardClient.isAtlas();
        logger.debug("sourceIsAtlas: {}, destIsAtlas: {}", sourceIsAtlas, destIsAtlas);

        Set<AtlasUser> existingUsers = new HashSet<>();
        if (destIsAtlas) {
            List<AtlasUser> users = atlasUtil.getDatabaseUsers(config.atlasProjectId);
            existingUsers.addAll(users);
        }

        Map<String, String> usersMap = readUsersInputCsv();

        CSVWriter writer = new CSVWriter(new FileWriter(config.getUsersOutputCsv()));
        String[] header = {"user", "password"};
        writer.writeNext(header);

        List<User> users = this.sourceShardClient.getUsers();
        for (User u : users) {
            String password = null;
            String type = null;
            AtlasUser atlasUser = new AtlasUser(u, password);

            if (destIsAtlas) {
                if (!u.getDb().equals("admin")) {
                    atlasUser.setUsername(u.getUser() + "_" + u.getDb());
                }

                if (usersMap != null && usersMap.containsKey(atlasUser.getUsername())) {
                    password = usersMap.get(atlasUser.getUsername());
                    type = "password from CSV";
                } else if (usersMap != null && usersMap.containsKey(u.getUser())) {
                    password = usersMap.get(u.getUser());
                    type = "password from CSV";
                } else {
                    password = RandomStringUtils.random(16, true, true);
                    type = "random password";
                }
                atlasUser.setPassword(password);

                if (existingUsers.contains(atlasUser)) {
                    if (usersMap != null && usersMap.containsKey(atlasUser.getUsername())) {
                        logger.debug("*** updating password for user {}", atlasUser.getUsername());
                        try {
                            atlasUtil.updateUser(config.atlasProjectId, atlasUser);
                        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
                            logger.error("syncUsers() error: {}", atlasUser, e);
                        }
                    } else {
                        logger.debug("Atlas user {} already exists", atlasUser.getUsername());
                    }

                } else {
                    try {
                        atlasUtil.createUser(config.atlasProjectId, atlasUser);
                    } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
                        logger.error("syncUsers() error: {}", atlasUser, e);
                    }
                }

            } else {
                if (destUserExists(u)) {

                    if (usersMap != null && usersMap.containsKey(u.getUser())) {
                        password = usersMap.get(u.getUser());
                        type = "password from CSV";
                    } else {
                        password = RandomStringUtils.random(16, true, true);
                        type = "random password";
                    }

                    Document updateUserCmd = new Document("updateUser", u.getUser()).append("pwd", password);
                    Document result = this.destShardClient.runCommand(updateUserCmd, u.getDb());
                    logger.debug("destination non-Atlas user {} updated with {}, result: {}", u.getUser(), type, result);
                } else {
                    logger.warn("user {} does not exist, not updating password", u.getUser());
                }

            }

            writer.writeNext(new String[]{atlasUser.getUsername(), password});

        }
        writer.flush();
        AtlasServiceGenerator.shutdown();
    }

    public void dropDestinationAtlasUsersAndRoles() {

        String excludeUser = null;
        MongoCredential credential = this.destShardClient.getConnectionString().getCredential();
        if (credential != null) {
            excludeUser = credential.getUserName();
        }
        atlasUtil.deleteUsers(config.atlasProjectId, excludeUser);
        atlasUtil.deleteRoles(config.atlasProjectId);
        AtlasServiceGenerator.shutdown();
    }


    public void syncMetadata() throws InterruptedException {
        logger.debug(String.format("Starting metadata sync/migration, %s: %s",
                ShardConfigSyncApp.NON_PRIVILEGED, config.nonPrivilegedMode));
        
        if (destShardClient.isVersion5OrLater()) {
        	throw new IllegalArgumentException("syncMetadata no longer supported for > 5.x, please use syncMetadataOptimized");
        }

        initChunkManager();
        stopBalancers();
        //checkAutosplit();
        createCollections(config);
        enableDestinationSharding();

        sourceShardClient.populateCollectionsMap();
        shardDestinationCollections();
        destShardClient.populateCollectionsMap();
        chunkManager.createDestChunksUsingSplitCommand();
        chunkManager.compareAndMoveChunks(true, false);

        if (!config.skipFlushRouterConfig) {
            destShardClient.flushRouterConfig();
        }
    }

    public void syncMetadataOptimized() {
        logger.debug(String.format("Starting optimized metadata sync/migration, %s: %s",
                ShardConfigSyncApp.NON_PRIVILEGED, config.nonPrivilegedMode));

        initChunkManager();
        stopBalancers();
        createCollections(config);
        enableDestinationSharding();
        sourceShardClient.populateCollectionsMap();
        shardDestinationCollections();
        destShardClient.populateCollectionsMap();

        chunkManager.createAndMoveChunks();

        if (!config.skipFlushRouterConfig) {
            destShardClient.flushRouterConfig();
        }
    }

    private void stopBalancers() {

        logger.debug("stopBalancers started");
        if (config.sourceClusterPattern == null) {
            try {
                sourceShardClient.stopBalancer();
            } catch (MongoCommandException mce) {
                logger.error("Could not stop balancer on source shard: " + mce.getMessage());
            }
        } else {
            logger.debug("Skipping source balancer stop, patterned uri");
        }

        if (config.destClusterPattern == null && !config.isShardToRs()) {
            try {
                destShardClient.stopBalancer();
            } catch (MongoCommandException mce) {
                logger.error("Could not stop balancer on dest shard: " + mce.getMessage());
            }
        } else {
            if (config.isShardToRs()) {
                logger.debug("Skipping dest balancer stop, destination is a replica set");
            } else {
                logger.debug("Skipping dest balancer stop, patterned uri");
            }

        }

        logger.debug("stopBalancers complete");
    }

    private void checkAutosplit() {
        sourceShardClient.checkAutosplit();
    }

    public void disableSourceAutosplit() {
        sourceShardClient.disableAutosplit();
    }


    public void compareChunksEquivalent() {
        initChunkManager();
        chunkManager.compareChunksEquivalent();
    }

    public void compareChunks() {
        initChunkManager();
        chunkManager.compareAndMoveChunks(false, false);
    }

    public void compareAndMoveChunks(boolean doMove, boolean ignoreMissing) {
        initChunkManager();
        chunkManager.compareAndMoveChunks(doMove, ignoreMissing);
    }

    @SuppressWarnings("unchecked")
    public void compareShardCounts() {

        logger.debug("Starting compareShardCounts mode");

        Document listDatabases = new Document("listDatabases", 1);
        Document sourceDatabases = sourceShardClient.adminCommand(listDatabases);
        Document destDatabases = destShardClient.adminCommand(listDatabases);

        List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
        List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

        populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
        populateDbMap(destDatabaseInfo, destDbInfoMap);

        for (Document sourceInfo : sourceDatabaseInfo) {
            String dbName = sourceInfo.getString("name");

            if (config.filtered && !config.getIncludeDatabasesAll().contains(dbName)
                    || dbName.equals("config") || dbName.equals("local") || dbName.equals("admin")) {
                logger.debug("Ignore " + dbName + " for compare, filtered");
                continue;
            }

            Document destInfo = destDbInfoMap.get(dbName);
            if (destInfo != null) {
                logger.debug(String.format("Found matching database %s", dbName));

                long sourceTotal = 0;
                long destTotal = 0;
                int collCount = 0;

                MongoDatabase sourceDb = sourceShardClient.getMongoClient().getDatabase(dbName);
                MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(dbName);
                MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
                for (String collectionName : sourceCollectionNames) {
                    if (collectionName.startsWith("system.")) {
                        continue;
                    }

                    Namespace ns = new Namespace(dbName, collectionName);
                    if (config.filtered && !config.getIncludeNamespaces().contains(ns)
                            && !config.getIncludeDatabases().contains(dbName)) {
//						logger.debug("include: " + includeNamespaces);
                        continue;
                    }


                    long[] result = doCounts(sourceDb, destDb, collectionName);
                    sourceTotal += result[0];
                    destTotal += result[1];
                    collCount++;
                }
                logger.debug("Database {} - source count sourceTotal: {}, dest count sourceTotal {}", dbName, sourceTotal, destTotal);
            } else {
                logger.warn(String.format("Destination db not found, name: %s", dbName));
            }
        }
    }

    public void cleanupPreviousShards(Set<String> shardNames) {

        logger.debug("Starting cleanupPreviousShards: [{}]", StringUtils.join(shardNames, ", "));

        Set<String> destShardNames = destShardClient.getShardsMap().keySet();
        boolean fatal = false;
        for (String shardName : shardNames) {
            if (!destShardNames.contains(shardName)) {
                logger.error("cleanupPreviousShards shardName {} not found on destination", shardName);
                fatal = true;
            }
        }
        if (fatal) {
            throw new IllegalArgumentException("cleanupPreviousShards: one or more shard names provided were not found on dest");
        }
        if (destShardNames.size() < 2) {
            throw new IllegalArgumentException("cleanupPreviousShards: 2 or more shards required on destination to use this option");
        }
        destShardClient.populateShardMongoClients();

        Document listDatabases = new Document("listDatabases", 1);
        Document destDatabases = destShardClient.adminCommand(listDatabases);

        List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

        populateDbMap(destDatabaseInfo, destDbInfoMap);

        MongoCollection<RawBsonDocument> destChunksColl = destShardClient.getChunksCollectionRaw();

        for (Document destInfo : destDatabaseInfo) {
            String dbName = destInfo.getString("name");

            MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(dbName);
            List<String> destCollectionNames = new ArrayList<>();

            destDb.listCollectionNames().into(destCollectionNames);
            for (String collectionName : destCollectionNames) {
                if (collectionName.startsWith("system.")) {
                    continue;
                }

                Namespace ns = new Namespace(dbName, collectionName);
                if (config.filterCheck(ns)) {
                    continue;
                }

                for (String shardName : shardNames) {

                    Set<String> t1 = new HashSet<>();
                    t1.add(shardName);
                    Set<String> otherShards = Sets.difference(destShardNames, t1);
                    if (!otherShards.isEmpty()) {

                        logger.debug("current shard: {}, otherShards: {}", shardName, otherShards);

                    }

                    MongoDatabase db = destShardClient.getShardMongoClient(shardName).getDatabase(dbName);

                    // find the first chunk that is on the shard where we are about to drop
                    RawBsonDocument firstChunk = destChunksColl.find(and(eq("ns", ns.getNamespace()), eq("shard", shardName))).first();

                    if (firstChunk != null) {
                        logger.debug("first chunk {}", firstChunk);
                        logger.debug("dropping {} on shard {}", ns, shardName);

                        String otherShard = otherShards.iterator().next();
                        boolean firstMove = destShardClient.moveChunk(firstChunk, otherShard, false);

                        if (firstMove) {
                            logger.debug("firstMove done");
                            db.getCollection(collectionName).drop();

                            // now move it back so that we get the UUID created correctly
                            destShardClient.moveChunk(firstChunk, shardName, false);
                        }


                    }


                }


            }
        }
        logger.debug("Finished cleanupPrevious");

    }

    public void cleanupPreviousAll() {

        logger.debug("Starting cleanupPreviousAll");

        Document listDatabases = new Document("listDatabases", 1);
        Document destDatabases = destShardClient.adminCommand(listDatabases);

        List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

        populateDbMap(destDatabaseInfo, destDbInfoMap);

        Document nullFilter = new Document();

        for (Document destInfo : destDatabaseInfo) {
            String dbName = destInfo.getString("name");

            MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(dbName);
            List<String> destCollectionNames = new ArrayList<>();

            destDb.listCollectionNames().into(destCollectionNames);
            for (String collectionName : destCollectionNames) {
                if (collectionName.startsWith("system.")) {
                    continue;
                }

                Namespace ns = new Namespace(dbName, collectionName);
                if (config.filterCheck(ns)) {
                    continue;
                }

                DeleteResult deleteResult = null;
                try {
                    deleteResult = destDb.getCollection(collectionName).deleteMany(nullFilter);
                } catch (MongoException me) {
                    logger.error("{}: delete error: {}", ns, me.getMessage());
                }

                if (deleteResult != null) {
                    long count = deleteResult.getDeletedCount();
                    if (count > 0) {
                        logger.debug("{}: deleted {} doucments on destination", ns, count);
                    }
                }

            }
        }
        logger.debug("Finished cleanupPrevious");
    }

    private long[] doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
        return doCounts(sourceDb, destDb, collectionName, null);
    }

    private long[] doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName, Bson query) {

        long[] result = new long[2];
        Long sourceCount = null;
        Long destCount = null;
        if (query == null) {
            sourceCount = sourceDb.getCollection(collectionName).countDocuments();
            destCount = destDb.getCollection(collectionName).countDocuments();
        } else {
            //db.getCollection(collectionName).countDocuments();
            sourceCount = sourceDb.getCollection(collectionName).countDocuments(query);
            destCount = destDb.getCollection(collectionName).countDocuments(query);
        }

        result[0] = sourceCount;
        result[1] = destCount;

        if (sourceCount.equals(destCount)) {
            logger.debug(String.format("%s.%s count matches: %s", sourceDb.getName(), collectionName, sourceCount));
            return result;
        } else {
            logger.warn(String.format("%s.%s count MISMATCH - source: %s, dest: %s, query: %s", sourceDb.getName(), collectionName,
                    sourceCount, destCount, query));
            return result;
        }
    }

    public void compareChunkCounts() {
        for (String databaseName : sourceShardClient.listDatabaseNames()) {
            MongoDatabase db = sourceShardClient.getMongoClient().getDatabase(databaseName);

            if (databaseName.equals("admin") || databaseName.equals("config")
                    || databaseName.contentEquals("local")) {
                continue;
            }

            for (Document collectionInfo : db.listCollections()) {
                String collectionName = (String) collectionInfo.get("name");
                if (collectionName.endsWith(".create")) {
                    continue;
                }

                Namespace ns = new Namespace(databaseName, collectionName);
                if (config.filtered && !config.getIncludeNamespaces().contains(ns)) {
                    logger.debug("compareChunkCounts skipping {}, filtered", ns);
                    continue;
                }
                compareChunkCounts(ns);
            }
        }
    }

    // TODO - this is incomplete
    public void compareChunkCounts(Namespace ns) {
        destShardClient.populateCollectionsMap();
        Document shardCollection = destShardClient.getCollectionsMap().get(ns.getNamespace());
        if (shardCollection == null) {
            logger.warn("Collection {} is not sharded, cannot do chunk compare", ns);
        } else {
            MongoDatabase sourceDb = sourceShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
            MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(ns.getDatabaseName());

            Document shardKeysDoc = (Document) shardCollection.get("key");
            Set<String> shardKeys = shardKeysDoc.keySet();

            // use dest chunks as reference, may be smaller
            MongoCollection<Document> chunksCollection = destShardClient.getChunksCollection();
            // int chunkCount = (int)sourceChunksColl.countDocuments(eq("ns",
            // ns.getNamespace()));

            FindIterable<Document> sourceChunks = chunksCollection.find(eq("ns", ns.getNamespace()))
                    .sort(Sorts.ascending("min"));
            for (Document sourceChunk : sourceChunks) {
                String id = sourceChunk.getString("_id");
                // each chunk is inclusive of min and exclusive of max
                Document min = (Document) sourceChunk.get("min");
                Document max = (Document) sourceChunk.get("max");
                Bson chunkQuery = null;

                if (shardKeys.size() > 1) {
                    List<Bson> filters = new ArrayList<Bson>(shardKeys.size());
                    for (String key : shardKeys) {
                        filters.add(and(gte(key, min.get(key)), lt(key, max.get(key))));
                    }
                    chunkQuery = and(filters);
                } else {
                    String key = shardKeys.iterator().next();
                    chunkQuery = and(gte(key, min.get(key)), lt(key, max.get(key)));
                }

                long[] result = doCounts(sourceDb, destDb, ns.getCollectionName(), chunkQuery);
            }
        }
    }

    public List<String> compareCollectionUuids() {
        String name = "dest";
        logger.debug(String.format("%s - Starting compareCollectionUuids", name));
        initChunkManager();
        destShardClient.populateShardMongoClients();

        List<String> failures = new ArrayList<>();
        List<String> dbNames = new ArrayList<>();
        destShardClient.listDatabaseNames().into(dbNames);

        Map<Namespace, Map<UUID, Set<String>>> collectionUuidMappings = new TreeMap<>();

        for (Map.Entry<String, MongoClient> entry : destShardClient.getShardMongoClients().entrySet()) {
            MongoClient client = entry.getValue();
            String shardName = entry.getKey();

            for (String databaseName : client.listDatabaseNames()) {
                MongoDatabase db = client.getDatabase(databaseName);

                if (databaseName.equals("admin") || databaseName.equals("config")
                        || databaseName.contentEquals("local")) {
                    continue;
                }

                for (Document collectionInfo : db.listCollections()) {
                    String collectionName = collectionInfo.getString("name");
                    String type = collectionInfo.getString("type");
                    if (collectionName.endsWith(".create") || "view".equals(type)) {
                        continue;
                    }
                    Namespace ns = new Namespace(databaseName, collectionName);

                    if (config.filterCheck(ns)) {
                        continue;
                    }

                    Document info = (Document) collectionInfo.get("info");
                    UUID uuid = (UUID) info.get("uuid");

                    Map<UUID, Set<String>> uuidMapping = collectionUuidMappings.get(ns);
                    if (uuidMapping == null) {
                        uuidMapping = new TreeMap<>();
                    }
                    collectionUuidMappings.put(ns, uuidMapping);

                    if (uuid == null) {
                        logger.error("Unexpected - uuid for {}.{} was null", databaseName, collectionName);
                    }
                    Set<String> shardNames = uuidMapping.get(uuid);
                    if (shardNames == null) {
                        shardNames = new HashSet<>();
                    }
                    uuidMapping.put(uuid, shardNames);
                    shardNames.add(shardName);

                    // logger.debug(entry.getKey() + " db: " + databaseName + "." + collectionName +
                    // " " + uuid);
                }
            }
        }

        int successCount = 0;
        int failureCount = 0;

        for (Map.Entry<Namespace, Map<UUID, Set<String>>> mappingEntry : collectionUuidMappings.entrySet()) {
            Namespace ns = mappingEntry.getKey();
            Map<UUID, Set<String>> uuidMappings = mappingEntry.getValue();
            if (uuidMappings.size() == 1) {
                successCount++;
                logger.debug(String.format("%s ==> %s", ns, uuidMappings));
            } else {
                failureCount++;
                String failureMessage = String.format("%s ==> %s", ns, uuidMappings);
                logger.error(failureMessage);
                failures.add(failureMessage);
                uuidFailure(ns, uuidMappings);
            }
        }

        if (failureCount == 0 && successCount > 0) {
            logger.debug(String.format("%s - compareCollectionUuids complete: successCount: %s, failureCount: %s", name,
                    successCount, failureCount));
        } else {
            logger.error(String.format("%s - compareCollectionUuids complete: successCount: %s, failureCount: %s", name,
                    successCount, failureCount));
        }
        return failures;
    }


    private void uuidFailure(Namespace ns, Map<UUID, Set<String>> uuidMappings) {
        Set<String> correctShards = chunkManager.getShardsForNamespace(ns);

        Set<String> allShards = new HashSet<>();
        for (Set<String> s : uuidMappings.values()) {
            allShards.addAll(s);
        }

        Set<String> incorrectShards = Sets.difference(allShards, correctShards);
        logger.debug("uuidFailure: {} - correct shards: {}, incorrect shards: {}", ns, correctShards, incorrectShards);

        for (String shardName : incorrectShards) {
            MongoClient client = destShardClient.getShardMongoClient(shardName);
            MongoCollection<Document> coll = client.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName());
            long count = coll.countDocuments();
            if (count == 0 && config.isDrop()) {
                logger.debug("dropping {} on shard (count = 0)", ns, shardName, count);
                coll.drop();
            } else {
                logger.debug("{} - {} - count: {}", ns, shardName, count);
            }


        }

    }

    private void enableSharding(String dbName, String primaryShard) {
        try {
            Document cmd = new Document("enableSharding", dbName);
            if (primaryShard != null) {
                cmd.append("primaryShard", primaryShard);
            }
            destShardClient.adminCommand(cmd);
        } catch (MongoCommandException mce) {
            if (mce.getCode() == 23 && mce.getErrorMessage().contains("sharding already enabled")) {
                logger.debug("Sharding already enabled: " + dbName);
            } else {
                throw mce;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void compareDatabaseMetadata() {
        initChunkManager();
        MongoCollection<Document> sourceDbs = sourceShardClient.getCollection("config.databases");
        MongoCollection<Document> destDbs = destShardClient.getCollection("config.databases");

        List<Document> sourceDatabaseInfo = new ArrayList<>();
        sourceDbs.find().into(sourceDatabaseInfo);
        List<Document> destDatabaseInfo = new ArrayList<>();
        destDbs.find().into(destDatabaseInfo);

        populateDbMap(sourceDatabaseInfo, sourceDbInfoMap, "_id");
        populateDbMap(destDatabaseInfo, destDbInfoMap, "_id");

        for (Document sourceInfo : sourceDatabaseInfo) {
            String dbName = sourceInfo.getString("_id");

            MongoDatabase db = sourceShardClient.getMongoClient().getDatabase(dbName);
            List<String> collNames = new ArrayList<>();
            db.listCollectionNames().into(collNames);

            if (config.filtered && !config.getIncludeDatabasesAll().contains(dbName)
                    || dbName.equals("config") || dbName.equals("local") || dbName.equals("admin")) {
                logger.debug("Ignore " + dbName + " for compare, filtered");
                continue;
            }

            String sourcePrimary = sourceInfo.getString("primary");
            String mappedPrimary = chunkManager.getShardMapping(sourcePrimary);

            Document destInfo = destDbInfoMap.get(dbName);
            if (destInfo == null) {
                logger.warn("Destination db not found, name: {}, collCount: {}", dbName, collNames.size());
                logger.debug("enableSharding on {}, in order to create on dest", dbName);
                enableSharding(dbName, mappedPrimary);
            } else {

                String destPrimary = destInfo.getString("primary");

                if (mappedPrimary.equals(destPrimary)) {
                    logger.debug("{} exists on source and dest", dbName);
                } else {
                    logger.warn("{} exists on source and dest, primary shard mismatch, collCount: {}, currentPrimary: {}, mappedPrimary: {}",
                            dbName, collNames.size(), destPrimary, mappedPrimary);
                }


            }
        }


    }

    private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap, String nameKey) {
        for (Document dbInfo : dbInfoList) {
            databaseMap.put(dbInfo.getString(nameKey), dbInfo);
        }
    }

    private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
        populateDbMap(dbInfoList, databaseMap, "name");
    }

    public void shardDestinationCollections() {
        // Don't use the insert method regardless, because that can cause us to
        // miss UUIDs for MongoDB 3.6+
        shardDestinationCollectionsUsingShardCommand();
    }

    private void shardDestinationCollectionsUsingInsert() {
        logger.debug("shardDestinationCollectionsUsingInsert(), privileged mode");

        MongoCollection<RawBsonDocument> destColls = destShardClient.getConfigDb().getCollection("collections",
                RawBsonDocument.class);
        ReplaceOptions options = new ReplaceOptions().upsert(true);

        for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {

            String nsStr = (String) sourceColl.get("_id");
            Namespace ns = new Namespace(nsStr);
            if (config.filterCheck(ns)) {
                continue;
            }

            // hack to avoid "Invalid BSON field name _id.x" for compound shard keys
            RawBsonDocument rawDoc = new RawBsonDocument(sourceColl, documentCodec);
            destColls.replaceOne(new Document("_id", nsStr), rawDoc, options);
        }

        logger.debug("shardDestinationCollectionsUsingInsert() complete");
    }

    private void shardDestinationCollectionsUsingShardCommand() {
        logger.debug("shardDestinationCollectionsUsingShardCommand(), non-privileged mode");

        for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {

            String nsStr = (String) sourceColl.get("_id");
            Namespace ns = new Namespace(nsStr);

            if (config.filterCheck(ns)) {
                continue;
            }
            shardCollection(sourceColl);

            if ((boolean) sourceColl.get("noBalance", false)) {
                // TODO there is no disableBalancing command so this is not
                // possible in Atlas
                // destClient.getDatabase("admin").runCommand(new Document("",
                // ""));
                logger.warn(String.format("Balancing is disabled for %s, this is not possible in Atlas", nsStr));
            }
        }
        logger.debug("shardDestinationCollectionsUsingShardCommand() complete");
        destShardClient.populateCollectionsMap(true);
    }

    /**
     * Take the sourceColl as a "template" to shard on the destination side
     *
     * @param sourceColl
     */
    private Document shardCollection(ShardCollection sourceColl) {
        Document shardCommand = new Document("shardCollection", sourceColl.getId());
        shardCommand.append("key", sourceColl.getKey());

        // apparently unique is not always correct here, there are cases where unique is
        // false
        // here but the underlying index is unique
        shardCommand.append("unique", sourceColl.isUnique());
        if (sourceColl.getDefaultCollation() != null) {
            shardCommand.append("collation", LOCALE_SIMPLE);
        }

        Document result = null;
        try {
            result = destShardClient.adminCommand(shardCommand);
        } catch (MongoCommandException mce) {
            if (mce.getCode() == 20) {
                logger.debug(String.format("Sharding already enabled for %s", sourceColl.getId()));
            } else {
                throw mce;
            }
        }
        return result;
    }

    private Document shardCollection(Document sourceColl) {
        Document shardCommand = new Document("shardCollection", sourceColl.get("_id"));

        Document key = (Document) sourceColl.get("key");
        shardCommand.append("key", key);

        // apparently unique is not always correct here, there are cases where unique is
        // false
        // here but the underlying index is unique
        shardCommand.append("unique", sourceColl.get("unique"));

        Object key1 = key.values().iterator().next();
        if ("hashed".equals(key1)) {
            shardCommand.append("numInitialChunks", 1);
        }

        if (sourceColl.get("defaultCollation", Document.class) != null) {
            shardCommand.append("collation", LOCALE_SIMPLE);
        }

        Document result = null;
        try {
            result = destShardClient.adminCommand(shardCommand);
        } catch (MongoCommandException mce) {
            if (mce.getCode() == 20) {
                logger.debug(String.format("Sharding already enabled for %s", sourceColl.get("_id")));
            } else {
                logger.error(String.format("Error sharding collection %s", sourceColl.get("_id")));
                //throw mce;
            }
        }
        return result;
    }

    /**
     * @param sync - THIS WILL shard on the dest side if not in sync
     */
    public void diffShardedCollections(boolean sync) {
        logger.debug("diffShardedCollections()");
        sourceShardClient.populateCollectionsMap();
        destShardClient.populateCollectionsMap();

        for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {

            String nsStr = (String) sourceColl.get("_id");
            Namespace ns = new Namespace(nsStr);
            if (config.filterCheck(ns)) {
                continue;
            }

            Document destCollection = destShardClient.getCollectionsMap().get(sourceColl.get("_id"));

            if (destCollection == null) {
                logger.debug("Destination collection not found: " + sourceColl.get("_id") + " sourceKey:"
                        + sourceColl.get("key"));
                if (sync) {
                    try {
                        Document result = shardCollection(sourceColl);
                        logger.debug("Sharded: " + result);
                    } catch (MongoCommandException mce) {
                        logger.error("Error sharding", mce);
                    }
                }
            } else {
                if (sourceColl.get("key").equals(destCollection.get("key"))) {
                    logger.debug("Shard key match for " + sourceColl);
                } else {
                    logger.warn("Shard key MISMATCH for " + sourceColl + " sourceKey:" + sourceColl.get("key")
                            + " destKey:" + destCollection.get("key"));
                }
            }
        }
    }

    public void enableDestinationSharding() {
        initChunkManager();
        sourceShardClient.populateShardMongoClients();

        logger.debug("enableDestinationSharding()");
        MongoCollection<Document> databasesColl = sourceShardClient.getConfigDb().getCollection("databases");

        // todo, what about unsharded collections, don't we need to movePrimary for
        // them?
        // FindIterable<Document> databases = databasesColl.find(eq("partitioned",
        // true));
        FindIterable<Document> databases = databasesColl.find();

        List<Document> databasesList = new ArrayList<Document>();
        databases.into(databasesList);
        for (Document database : databasesList) {
            String databaseName = database.getString("_id");
            if (databaseName.equals("admin") || databaseName.equals("system") || databaseName.equals("local")
                    || databaseName.contains("$")) {
                continue;
            }
            if (config.filtered && !config.getIncludeDatabasesAll().contains(databaseName)) {
                logger.trace("Database " + databaseName + " filtered, not sharding on destination");
                continue;
            }
            String primary = database.getString("primary");
            //String xx = sourceToDestShardMap.get(primary);
            String mappedPrimary = chunkManager.getShardMapping(primary);
            logger.debug("database: " + databaseName + ", primary: " + primary + ", mappedPrimary: " + mappedPrimary);
            if (mappedPrimary == null) {
                logger.warn("Shard mapping not found for shard " + primary);
            }

            Document dest = destShardClient.getConfigDb().getCollection("databases")
                    .find(new Document("_id", databaseName)).first();
            if (database.getBoolean("partitioned", true)) {
                logger.debug(String.format("enableSharding: %s", databaseName));
                enableSharding(databaseName, null);
            }

            // this needs to be the atlas-xxx id
            String shardId = chunkManager.getDestToSourceShardMapping(mappedPrimary);
            MongoClient primaryClient = sourceShardClient.getShardMongoClient(shardId);
            List<String> primaryDatabasesList = new ArrayList<String>();
            try {
                primaryClient.listDatabaseNames().into(primaryDatabasesList);
            } catch (MongoCommandException mce) {
                if (mce.getCode() == 13) {
                    String coll = primaryClient.getDatabase(databaseName).listCollectionNames().first();
                    logger.debug("{} first collection {}", databaseName, coll);
                    if (coll == null) {
                        logger.debug("Database: " + databaseName + " does not exist on source shard, skipping");
                        continue;
                    }
                } else {
                    throw mce;
                }
            }

            if (!primaryDatabasesList.contains(databaseName)) {
                logger.debug("Database: " + databaseName + " does not exist on source shard, skipping");
                continue;
            }

            dest = destShardClient.createDatabase(databaseName);

            String destPrimary = dest.getString("primary");
            if (mappedPrimary.equals(destPrimary)) {
                logger.debug("Primary shard already matches for database: " + databaseName);
            } else {
                logger.debug(
                        "movePrimary for database: " + databaseName + " from " + destPrimary + " to " + mappedPrimary);
                try {
                    destShardClient.adminCommand(new Document("movePrimary", databaseName).append("to", mappedPrimary));
                } catch (MongoCommandException mce) {
                    // TODO check if exists on source rather than this
                    logger.warn("movePrimary for database: " + databaseName + " failed. Maybe it doesn't exist?");
                }
            }

        }
        logger.debug("enableDestinationSharding() complete");
    }

    /**
     * Drop based on config.databases
     */
    public void dropDestinationDatabases() {
        logger.debug("dropDestinationDatabases()");
        destShardClient.populateShardMongoClients();
        MongoCollection<Document> databasesColl = sourceShardClient.getDatabasesCollection();
        FindIterable<Document> databases = databasesColl.find();
        List<String> databasesList = new ArrayList<String>();

        for (Document database : databases) {
            String databaseName = database.getString("_id");

            if (config.filtered && !config.getIncludeDatabases().contains(databaseName)) {
                logger.trace("Database " + databaseName + " filtered, not dropping on destination");
                continue;
            } else {
                databasesList.add(databaseName);
            }
        }
        destShardClient.dropDatabases(databasesList);
        logger.debug("dropDestinationDatabases() complete");
    }

    public void dropDestinationDatabasesAndConfigMetadata() {
        logger.debug("dropDestinationDatabasesAndConfigMetadata()");
        destShardClient.populateShardMongoClients();
        MongoCollection<Document> databasesColl = sourceShardClient.getDatabasesCollection();
        FindIterable<Document> databases = databasesColl.find();
        List<String> databasesList = new ArrayList<String>();

        for (Document database : databases) {
            String databaseName = database.getString("_id");

            if (config.filtered && !config.getIncludeDatabases().contains(databaseName)) {
                logger.trace("Database " + databaseName + " filtered, not dropping on destination");
                continue;
            } else {
                databasesList.add(databaseName);
            }
        }
        destShardClient.dropDatabasesAndConfigMetadata(databasesList);
        logger.debug("dropDestinationDatabasesAndConfigMetadata() complete");

    }

    public void cleanupOrphans() {
        logger.debug("cleanupOrphans()");
        sourceShardClient.populateCollectionsMap();
        sourceShardClient.populateShardMongoClients();
        CleanupOrphaned cleaner = new CleanupOrphaned(sourceShardClient, config.getIncludeNamespaces());
        cleaner.cleanupOrphans(config.cleanupOrphansSleepMillis);
    }

    public void cleanupOrphansDest() {
        logger.debug("cleanupOrphansDest()");
        destShardClient.populateCollectionsMap();
        destShardClient.populateShardMongoClients();
        CleanupOrphaned cleaner = new CleanupOrphaned(destShardClient, config.getIncludeNamespaces());
        cleaner.cleanupOrphans(config.cleanupOrphansSleepMillis);
    }


    public void shardToRs() throws ExecuteException, IOException {

        logger.debug("shardToRs() starting");
        stopBalancers();

        List<MongoMirrorRunner> mongomirrors = new ArrayList<>(sourceShardClient.getShardsMap().size());
        int httpStatusPort = config.mongoMirrorStartPort;
        for (Shard source : sourceShardClient.getShardsMap().values()) {
            logger.debug("sourceShard: " + source.getId());
            MongoMirrorRunner mongomirror = new MongoMirrorRunner(source.getId());
            mongomirrors.add(mongomirror);

            // Source setup
            mongomirror.setSourceHost(source.getHost());

            MongoCredential sourceCredentials = sourceShardClient.getConnectionString().getCredential();
            if (sourceCredentials != null) {
                mongomirror.setSourceUsername(sourceCredentials.getUserName());
                mongomirror.setSourcePassword(new String(sourceCredentials.getPassword()));
                mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
            }
            if (sourceShardClient.getConnectionString().getSslEnabled() != null) {
                mongomirror.setSourceSsl(sourceShardClient.getConnectionString().getSslEnabled());
            }

            ClusterDescription cd = destShardClient.getMongoClient().getClusterDescription();
            ServerDescription s1 = cd.getServerDescriptions().get(0);
            String setName = s1.getSetName();


            // destMongoClientURI.getCredentials().getSource();
            ConnectionString cs = destShardClient.getConnectionString();
            String host = destShardClient.getConnectionString().getHosts().get(0); // TODO verify

            mongomirror.setDestinationHost(setName + "/" + host);
            MongoCredential destCredentials = destShardClient.getConnectionString().getCredential();
            if (destCredentials != null) {
                mongomirror.setDestinationUsername(destCredentials.getUserName());
                mongomirror.setDestinationPassword(new String(destCredentials.getPassword()));
                mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
            }

            if (destShardClient.getConnectionString().getSslEnabled() == null
                    || destShardClient.getConnectionString().getSslEnabled().equals(Boolean.FALSE)) {
                // TODO - this is only in "hacked" mongomirror
                mongomirror.setDestinationNoSSL(true);
            }

            for (Namespace ns : config.getIncludeNamespaces()) {
                mongomirror.addIncludeNamespace(ns);
            }

            for (String dbName : config.getIncludeDatabases()) {
                mongomirror.addIncludeDatabase(dbName);
            }

//            if (dropDestinationCollectionsIfExisting) {
//                if (! destShard.isMongomirrorDropped()) {
//                    // for n:m shard mapping, only set drop on the first mongomiirror that we start,
//                    // since there will be multiple mongomirrors pointing to the same destination
//                    // and we would drop data that had started to copy
//                    mongomirror.setDrop(dropDestinationCollectionsIfExisting);
//                    destShard.setMongomirrorDropped(true);
//                }
//            }

            mongomirror.setMongomirrorBinary(config.mongomirrorBinary);

            String dateStr = formatter.format(LocalDateTime.now());

            // TODO
            //mongomirror.setBookmarkFile(String.format("%s_%s.timestamp", source.getId(), dateStr));
            mongomirror.setBookmarkFile(source.getId() + ".timestamp");

            mongomirror.setNumParallelCollections(config.numParallelCollections);
            mongomirror.setHttpStatusPort(httpStatusPort++);

            setMongomirrorEmailReportDetails(mongomirror);

            mongomirror.execute(config.dryRun);
            try {
                Thread.sleep(config.sleepMillis);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        pollMongomirrorStatus(mongomirrors);

    }

    private void writeTimestampFile(Shard shard, String startingTs) throws IOException {
        ShardTimestamp st = sourceShardClient.populateLatestOplogTimestamp(shard, startingTs);
        logger.debug(st.toJsonString());
        try {
            File tsFile = new File(shard.getId() + ".timestamp");
            BufferedWriter writer = new BufferedWriter(new FileWriter(tsFile));
            writer.write(st.toJsonString());
            writer.newLine();
            writer.close();

        } catch (IOException e) {
            logger.error(String.format("Error writing timestamp file for shard %s", shard.getId()), e);
            throw e;
        }
    }

    public void mongomirrorTailFromLatestOplogTs(String startingTs) throws IOException {
        logger.debug("Starting mongomirrorTailFromTs, startingTs: {}", startingTs);
        sourceShardClient.populateShardMongoClients();
        Collection<Shard> shards = sourceShardClient.getShardsMap().values();

        if (shards.isEmpty()) {
            Document isMasterResult = sourceShardClient.getMongoClient().getDatabase("admin").runCommand(new Document("isMaster", 1));
            String shardId = isMasterResult.getString("setName");
            Shard shard = new Shard();
            shard.setId(shardId);
            shard.setRsName(shardId);
            writeTimestampFile(shard, startingTs);
        } else {
            for (Shard shard : shards) {
                writeTimestampFile(shard, startingTs);
            }
        }

        mongomirror();
    }

    public void mongomirrorTailFromTs(String ts) throws IOException {
        String[] tsParts = ts.split(",");
        int seconds = Integer.parseInt(tsParts[0]);
        int increment = Integer.parseInt(tsParts[1]);
        BsonTimestamp bsonTs = new BsonTimestamp(seconds, increment);
        mongomirrorTailFromTs(bsonTs);
    }

    public void mongomirrorTailFromNow() throws IOException {


        long now = System.currentTimeMillis();
        long nowSeconds = now / 1000l;
        BsonTimestamp nowBson = new BsonTimestamp((int) nowSeconds, 1);
        logger.debug(String.format("Starting mongomirrorTailFromTs, now: %s, nowSeconds: %s, nowBson: %s",
                now, nowSeconds, nowBson));
        mongomirrorTailFromTs(nowBson);
    }

    private void mongomirrorTailFromTs(BsonTimestamp nowBson) throws IOException {


        //sourceShardClient.populateShardMongoClients();
        Collection<Shard> shards = sourceShardClient.getShardsMap().values();
        logger.debug("shardCount: " + shards.size());

        for (Shard shard : shards) {
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(new File(shard.getId() + ".timestamp")));
                writer.write(shard.getRsName());
                writer.newLine();
                writer.write(String.valueOf(nowBson.getValue()));
                writer.close();

            } catch (IOException e) {
                logger.error(String.format("Error writing timestamp file for shard %s", shard.getId()), e);
                throw e;
            }
        }
        mongomirror();
    }

    public void mongomirror() throws ExecuteException, IOException {
        initChunkManager();
        destShardClient.populateShardMongoClients();

        List<MongoMirrorRunner> mongomirrors = new ArrayList<>(sourceShardClient.getShardsMap().size());

        int httpStatusPort = config.mongoMirrorStartPort;

        for (Shard source : sourceShardClient.getShardsMap().values()) {

            MongoMirrorRunner mongomirror = new MongoMirrorRunner(source.getId());
            mongomirrors.add(mongomirror);

            mongomirror.setSourceHost(source.getHost());

            MongoCredential sourceCredentials = sourceShardClient.getConnectionString().getCredential();
            if (sourceCredentials != null) {
                mongomirror.setSourceUsername(sourceCredentials.getUserName());
                mongomirror.setSourcePassword(new String(sourceCredentials.getPassword()));
                mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
            }

            if (config.sourceRsSsl != null) {
                mongomirror.setSourceSsl(config.sourceRsSsl);
            } else if (sourceShardClient.getConnectionString().getSslEnabled() != null) {
                mongomirror.setSourceSsl(sourceShardClient.getConnectionString().getSslEnabled());
            }

            // Destination setup
            ClusterDescription cd = destShardClient.getMongoClient().getClusterDescription();

            // destMongoClientURI.getCredentials().getSource();
            String destShardId = chunkManager.getShardMapping(source.getId());
            Shard dest = destShardClient.getShardsMap().get(destShardId);
            String host = dest.getHost();

            logger.debug(String.format("Creating MongoMirrorRunner for %s ==> %s", source.getId(), dest.getId()));

            mongomirror.setDestinationHost(host);

            MongoCredential destCredentials = destShardClient.getConnectionString().getCredential();
            if (destCredentials != null) {
                mongomirror.setDestinationUsername(destCredentials.getUserName());
                mongomirror.setDestinationPassword(new String(destCredentials.getPassword()));
                mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
            }

            if (destShardClient.getConnectionString().getSslEnabled() == null
                    || destShardClient.getConnectionString().getSslEnabled().equals(Boolean.FALSE)) {
                // TODO - this is only in "hacked" mongomirror
                mongomirror.setDestinationNoSSL(true);
            }
            mongomirror.setExtendTtl(config.extendTtl);

            for (Namespace ns : config.getIncludeNamespaces()) {
                mongomirror.addIncludeNamespace(ns);
            }

            for (String dbName : config.getIncludeDatabases()) {
                mongomirror.addIncludeDatabase(dbName);
            }

            mongomirror.setMongomirrorBinary(config.mongomirrorBinary);
            mongomirror.setBookmarkFile(source.getId() + ".timestamp");

            mongomirror.setPreserveUUIDs(config.preserveUUIDs);
            mongomirror.setNumParallelCollections(config.numParallelCollections);
            mongomirror.setWriteConcern(config.writeConcern);
            mongomirror.setHttpStatusPort(httpStatusPort++);

            logger.debug("noIndexRestore=" + config.noIndexRestore);
            if (config.noIndexRestore) {
                mongomirror.setNoIndexRestore(config.noIndexRestore);
            }
            if (config.compressors != null) {
                mongomirror.setCompressors(config.compressors);
            }
            if (config.oplogBasePath != null) {
                mongomirror.setOplogPath(String.format("%s/%s", config.oplogBasePath, source.getId()));
            }
            if (config.collStatsThreshold != null) {
                mongomirror.setCollStatsThreshold(config.collStatsThreshold);
            }
            setMongomirrorEmailReportDetails(mongomirror);

            mongomirror.execute(config.dryRun);

            try {
                Thread.sleep(config.sleepMillis);
            } catch (InterruptedException e) {
            }
        }

        pollMongomirrorStatus(mongomirrors);

    }

    private void setMongomirrorEmailReportDetails(MongoMirrorRunner mmr) {
        if (config.emailReportRecipients == null) {
            return;
        }
        for (String emailRecipient : config.emailReportRecipients) {
            mmr.addEmailRecipient(emailRecipient);
        }
        mmr.setSmtpHost(config.smtpHost);
        mmr.setSmtpPort(config.smtpPort);
        mmr.setSmtpTls(config.smtpStartTlsEnable);
        mmr.setSmtpAuth(config.smtpAuth);
        mmr.setEmailFrom(config.mailFrom);
        mmr.setSmtpPassword(config.smtpPassword);
        mmr.setErrMsgWindowSecs(config.errorMessageWindowSecs);
        mmr.setErrorRptMaxErrors(config.errorReportMax);
        mmr.setTotalEmailsMax(config.emailReportMax);
        if (config.stopWhenLagWithin > 0) {
            mmr.setStopWhenLagWithin(config.stopWhenLagWithin);
        }
    }

    public void pollMongomirrorStatus(List<MongoMirrorRunner> mongomirrors) {
        if (config.dryRun) {
            return;
        }

        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            for (MongoMirrorRunner mongomirror : mongomirrors) {
                MongoMirrorStatus status = mongomirror.checkStatus();
                if (status == null) {
                    continue;
                }
                if (status.getErrorMessage() != null) {
                    logger.error("{} - mongomirror error, count={}, {}", mongomirror.getId(),
                            mongomirror.getErrorCount(), status.getErrorMessage());
                } else if (status.isInitialSync()) {
                    MongoMirrorStatusInitialSync st = (MongoMirrorStatusInitialSync) status;
                    if (st.isCopyingIndexes()) {
                        logger.debug(String.format("%-15s - %-18s %-22s", mongomirror.getId(), status.getStage(),
                                status.getPhase()));
                    } else {
                        double cs = st.getCompletionPercent();
                        logger.debug(String.format("%-15s - %-18s %-22s %6.2f%% complete", mongomirror.getId(),
                                status.getStage(), status.getPhase(), cs));
                    }

                } else if (status.isOplogSync()) {
                    MongoMirrorStatusOplogSync st = (MongoMirrorStatusOplogSync) status;
                    logger.debug(String.format("%-15s - %-18s %-22s %s lag from source", mongomirror.getId(),
                            status.getStage(), status.getPhase(), st.getLagPretty()));
                } else {
                    logger.debug(String.format("%-15s - %-18s %-22s", mongomirror.getId(), status.getStage(),
                            status.getPhase()));
                }

            }
        }
    }

    public void setMongomirrorBinary(String binaryPath) {
        if (binaryPath != null) {
            this.config.mongomirrorBinary = new File(binaryPath);
        }
    }

	public void setChunkManager(ChunkManager chunkManager) {
		this.chunkManager = chunkManager;
	}

}
