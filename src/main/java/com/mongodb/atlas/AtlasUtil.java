package com.mongodb.atlas;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static com.mongodb.util.spring.DigestAuthFilterFunction.setDigestAuth;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClient;

import com.mongodb.atlas.model.AtlasRole;
import com.mongodb.atlas.model.AtlasRoleReference;
import com.mongodb.atlas.model.AtlasRoleResponse;
import com.mongodb.atlas.model.AtlasUser;
import com.mongodb.atlas.model.AtlasUsersResponse;
import com.mongodb.client.MongoClient;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import me.vzhilin.auth.DigestAuthenticator;

public class AtlasUtil {

	private static final String BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0";

	private final static String[] FTDC_LOGS = { "FTDC" };

	private static Logger logger = LoggerFactory.getLogger(AtlasUtil.class);
	
	private final static AtlasRoleReference atlasAdmin = new AtlasRoleReference("atlasAdmin", "admin");

	private AtlasApi service;

	private CodecRegistry pojoCodecRegistry;
	private MongoClient mongoClient;

	private String apiPublicKey;
	private String apiPrivateKey;
	
	private SSLContext sslContext;

	private DescriptiveStatistics diskStats = new DescriptiveStatistics();
	
	private HttpAuthenticationFeature feature;
	
	private Client client;
	
	private DigestAuthenticator digestAuthenticator;

	public AtlasUtil(String username, String apiKey) throws KeyManagementException, NoSuchAlgorithmException {
		service = AtlasServiceGenerator.createService(AtlasApi.class, username, apiKey);
		this.apiPublicKey = username;
		this.apiPrivateKey = apiKey;
		init();
	}

	private void init() throws KeyManagementException, NoSuchAlgorithmException {
		pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
		
		TrustManager[] trustManager = new X509TrustManager[] { new X509TrustManager() {

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			@Override
			public void checkClientTrusted(X509Certificate[] certs, String authType) {

			}

			@Override
			public void checkServerTrusted(X509Certificate[] certs, String authType) {

			}
		} };

		sslContext = SSLContext.getInstance("SSL");
		sslContext.init(null, trustManager, null);
		
		feature = HttpAuthenticationFeature.universalBuilder()
				.credentialsForBasic(apiPublicKey, apiPrivateKey).credentials(apiPublicKey, apiPrivateKey).build();

		ClientConfig clientConfig = new ClientConfig().register(new JacksonJsonProvider());
		client = ClientBuilder.newBuilder().sslContext(sslContext).withConfig(clientConfig).build();
		
		digestAuthenticator = new DigestAuthenticator(apiPublicKey, apiPrivateKey);
	}

//    public List<Project> getProjects() throws IOException {
//    	Call<ProjectsResult> callSync = service.getProjects();
//    	Response<ProjectsResult> response = callSync.execute();
//    	ProjectsResult projects = response.body();
//    	return projects.getProjects();
//    }
//
//    public List<Cluster> getClusters(String groupId) throws IOException {
//
//        Call<ClustersResult> callSync = service.getClusters(groupId);
//        Response<ClustersResult> response = callSync.execute();
//        ClustersResult clusters = response.body();
//        return clusters.getClusters();
//    }
//    
//    public Cluster getCluster(String groupId, String clusterName) throws IOException {
//
//        Call<Cluster> callSync = service.getCluster(groupId, clusterName);
//        Response<Cluster> response = callSync.execute();
//        Cluster cluster = response.body();
//        return cluster;
//    }
//
//    public List<com.mongodb.atlas.model.Process> getProcesses(String groupId) throws IOException {
//
//        Call<ProcessesResult> callSync = service.getProcesses(groupId);
//        
//        
//        Response<ProcessesResult> response = callSync.execute();
//        
//        ProcessesResult procs = response.body();
//        
//        int total = procs.getTotalCount();
//        
//        List<com.mongodb.atlas.model.Process> procList = procs.getProcesses();
//        if (total > procList.size()) {
//            List<com.mongodb.atlas.model.Process> result = new ArrayList<com.mongodb.atlas.model.Process>();
//            result.addAll(procList);
//            int processed = procList.size();
//            int pageNum = 2;
//            while (processed < total) {
//                callSync = service.getProcesses(groupId, pageNum++);
//                response = callSync.execute();
//                procs = response.body();
//                procList = procs.getProcesses();
//                processed += procList.size();
//                result.addAll(procList);
//            }
//            return result;
//            
//        } else {
//            return procList; 
//        }
//        
//    }

//	LoggingFeature logging() {
//		Logger logger = Logger.getLogger(this.getClass().getName());
//		return new LoggingFeature(logger, Level.INFO, null, null);
//	}
	
	public void deleteCustomDbRole(String groupId, String roleName) {
		
		WebTarget webTarget = client.target(BASE_URL).path("groups").path(groupId).path("customDBRoles").path("roles");

		webTarget.register(feature);
		Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
		Response response = invocationBuilder.delete();
		String str = response.readEntity(String.class);
		logger.debug("delete custom role result: {}", str);
		
	}
	
	public List<AtlasRole> getCustomDbRoles(String groupId) {
		
		WebTarget webTarget = client.target(BASE_URL).path("groups").path(groupId)
				.path("customDBRoles").path("roles");
		webTarget.register(feature);
		Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
		Response response = invocationBuilder.get();
		
		if (response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
			logger.debug("getCustomDbRoles(), response code: {}", response.getStatus());
			List<AtlasRole> roles = response.readEntity(new GenericType<List<AtlasRole>>() {});
			//String resp = response.readEntity(String.class);
			//logger.debug("getCustomDbRoles(), response string: {}", resp);
			return roles;
		} else {
			logger.error("getCustomDbRoles error: {}", response.getStatusInfo());
			return null;
		}
	}
	
	public List<AtlasUser> getDatabaseUsers(String groupId) {
		
		WebTarget webTarget = client.target(BASE_URL).path("groups").path(groupId)
				.path("databaseUsers").queryParam("itemsPerPage", "500");

		webTarget.register(feature);
		Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
		Response response = invocationBuilder.get();
		AtlasUsersResponse dbUsersResponse = response.readEntity(AtlasUsersResponse.class);
		return dbUsersResponse.getResults();
	}

	public AtlasRoleResponse createCustomDbRole(String groupId, AtlasRole role)
			throws IOException, NoSuchAlgorithmException, KeyManagementException {

		// "groups/{groupId}/customDBRoles/roles"
		WebTarget webTarget = client.target(BASE_URL).path("groups").path(groupId).path("customDBRoles").path("roles");

		webTarget.register(feature);
		Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
		Response response = invocationBuilder.post(Entity.entity(role, MediaType.APPLICATION_JSON));

		try {
			if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
				//AtlasRole atlasRoleResult = response.readEntity(AtlasRole.class);
				return AtlasRoleResponse.newSuccessResponse(null);
			} else if (response.getStatus() == 409) {
				String resp = response.readEntity(String.class);
				return AtlasRoleResponse.newDuplicateResponse(resp);
			} else {

				String resp = response.readEntity(String.class);
				return AtlasRoleResponse.newFailedResponse(resp);
			}
		} finally {
			response.close();
		}
	}

	public void deleteRoles(String atlasProjectId) {
		List<AtlasRole> roles = getCustomDbRoles(atlasProjectId);
		
		for (AtlasRole role : roles) {
			
			WebTarget webTarget = client.target(BASE_URL).path("groups").path(atlasProjectId).path("customDBRoles").path("roles").path(role.getRoleName());

			webTarget.register(feature);
			Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
			
			Response response = invocationBuilder.delete();
			String str = response.readEntity(String.class);
			
			if (response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
				logger.debug("deleted role: {}", str);
			} else {
				logger.error("delete role error: {}", str);
			}
		}
		
	}
	
	public void deleteUsers(String atlasProjectId, String excludeUser) {
		List<AtlasUser> users = getDatabaseUsers(atlasProjectId);
		
		for (AtlasUser user : users) {
			
			String username = user.getUsername();
			if (username.equals(excludeUser)) {
				logger.debug("skipping user delete for excluded user: {}", username);
				continue;
			} else if (user.hasRole(atlasAdmin)) {
				logger.debug("skipping user delete for user with atlasAdmin: {}", username);
				continue;
			}
			
			user.getRoles();
			
			WebTarget webTarget = client.target(BASE_URL).path("groups").path(atlasProjectId).path("databaseUsers").path("admin").path(username);

			webTarget.register(feature);
			Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
			
			Response response = invocationBuilder.delete();
			String str = response.readEntity(String.class);
			
			if (response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
				logger.debug("deleted user: {}", username);
			} else {
				logger.error("delete user: {} error: {}", username, str);
			}
		}
	}
	
	public void deleteUsers(String atlasProjectId) {
		this.deleteUsers(atlasProjectId, null);
		
	}
	
	public void createUser(String groupId, AtlasUser user)
			throws IOException, NoSuchAlgorithmException, KeyManagementException {

		// "groups/{groupId}/customDBRoles/roles"
		WebTarget webTarget = client.target(BASE_URL).path("groups").path(groupId).path("databaseUsers");

		webTarget.register(feature);
		Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
		Response response = invocationBuilder.post(Entity.entity(user, MediaType.APPLICATION_JSON));

		try {
			if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
				logger.debug("created user: {}", user.getUsername());
			} else if (response.getStatus() == 409) {
				String resp = response.readEntity(String.class);
				logger.debug("user {} already exists: {}", user.getUsername(), resp);
			} else {

				String resp = response.readEntity(String.class);
				logger.error("user {} create failed: {}", user.getUsername(), resp);
			}
		} finally {
			response.close();
		}
	}

	public void updateUser(String groupId, AtlasUser user)
			throws IOException, NoSuchAlgorithmException, KeyManagementException {
		
		WebClient webClient = WebClient.builder()
		        .baseUrl(BASE_URL)
		        .filters(setDigestAuth(digestAuthenticator))
		        .defaultHeaders(header -> header.setBasicAuth(apiPublicKey, apiPrivateKey))
		        .build();
		
		String response = webClient
        .patch()
        .uri(uriBuilder -> uriBuilder
        	    .path("/groups/{groupId}/databaseUsers/admin/{username}")
        	    .build(groupId, user.getUsername()))
        .bodyValue(user)
        .retrieve()
        .bodyToMono(String.class).block();

		logger.debug("#### {}", response);
	}
	
//    public void getLogs(String groupId, String hostId, long startDate) throws IOException {
//    	LogCollectionJobRequest req = new LogCollectionJobRequest();
//    	req.setResourceType("REPLICASET");
//    	req.setResourceName(hostId);
//    	req.setLogTypes(FTDC_LOGS);
//    	Call<LogCollectionJob> logCall = service.startLogCollectionJob(groupId, req);
//    	Response<LogCollectionJob> logResponse = logCall.execute();
//    	LogCollectionJob job =  logResponse.body();
//    	
////    	Call<ResponseBody> callSync = service.getFtdc(groupId, hostId, startDate);
////        Response<ResponseBody> response = callSync.execute();
////        ResponseBody body = response.body();
////        File file = new File(hostId);
////        InputStream inputStream = null;
////        OutputStream outputStream = null;
////
////        try {
////            byte[] fileReader = new byte[4096];
////
////            long fileSize = body.contentLength();
////            long fileSizeDownloaded = 0;
////
////            inputStream = body.byteStream();
////            outputStream = new FileOutputStream(file);
////
////            while (true) {
////                int read = inputStream.read(fileReader);
////
////                if (read == -1) {
////                    break;
////                }
////
////                outputStream.write(fileReader, 0, read);
////
////                fileSizeDownloaded += read;
////
////                logger.debug("file download: " + fileSizeDownloaded + " of " + fileSize);
////            }
////
////            outputStream.flush();
////        } catch (IOException e) {
////        	logger.error("Error processing FTDC download", e);
////        } finally {
////            if (inputStream != null) {
////                inputStream.close();
////            }
////
////            if (outputStream != null) {
////                outputStream.close();
////            }
////            if (body != null) {
////            	body.close();
////            }
////        }
//    }
//
//    public void getMeasurements(String groupId, String hostId) throws IOException {
//        Call<MeasurementsResult> callSync = service.getMeasurements(groupId, hostId);
//        Response<MeasurementsResult> response = callSync.execute();
//        MeasurementsResult result = response.body();
//        if (result != null) {
//            List<Measurement> measurements = result.getMeasurements();
//            for (Measurement m : measurements) {
//                for (MeasurementDataPoint d : m.getDataPoints()) {
//                    if (d.getValue() != null) {
//                        System.out.println(d);
//                    }
//                    
//                }
//            }
//        }
//    }
//    
//    public DescriptiveStatistics getDiskStats(String groupId, String processId) throws IOException {
//        diskStats.clear();
//        Call<MeasurementsResult> callSync = service.getDiskMeasurements(groupId, processId);
//        Response<MeasurementsResult> response = callSync.execute();
//        MeasurementsResult result = response.body();
//        if (result != null) {
//            List<Measurement> measurements = result.getMeasurements();
//            for (Measurement m : measurements) {
//                for (MeasurementDataPoint d : m.getDataPoints()) {
//                    if (d.getValue() != null) {
//                        //System.out.println(d);
//                        diskStats.addValue(d.getValue());
//                    }
//                }
//            }
//        }
//        return diskStats;
//    }
//    
//    public List<String> getDatabaseNames(String groupId, String processId) throws IOException {
//        ArrayList<String> results = new ArrayList<String>();
//        Call<DatabasesResult> callSync = service.getDatabases(groupId, processId);
//        Response<DatabasesResult> response = callSync.execute();
//        DatabasesResult result = response.body();
//        if (result != null) {
//            for (Database db : result.getDatabases()) {
//                if (db.getDatabaseName().equals("local")) {
//                    continue;
//                }
//                //System.out.println("    " + db.getDatabaseName());
//                results.add(db.getDatabaseName());
//            }
//        }
//        return results;
//    }
//    
//    private double getDbMeasurement(Measurement m) {
//        double total = 0.0;
//        for (MeasurementDataPoint d : m.getDataPoints()) {
//            if (d.getValue() != null) {
//                if (!m.getUnits().equals("BYTES")) {
//                    throw new RuntimeException("Unexpected unit: " + m.getUnits());
//                }
//                //System.out.println(d.getValue());
//                double gb = ByteSizesUtil.bytesToGigabytes(d.getValue());
//                total += gb;
//            } else {
//                throw new RuntimeException("null dataPoint, WTF!?");
//            }
//        }
//        return total;
//    }
//    
//    public DatabasesStats getDatabasesStats(String groupId, String hostId) throws IOException {
//        DatabasesStats stats = new DatabasesStats();
//        List<String> databases = getDatabaseNames(groupId, hostId);
//        for (String dbName : databases) {
//            Call<MeasurementsResult> callSync = service.getDatabaseMeasurements(groupId, hostId, dbName);
//            Response<MeasurementsResult> response = callSync.execute();
//            MeasurementsResult result = response.body();
//            if (result != null) {
//                List<Measurement> measurements = result.getMeasurements();
//                for (Measurement m : measurements) {
//                    // System.out.println(m.getName());
//                    if (m.getName().equals("DATABASE_DATA_SIZE")) {
//                        //double d = m.getDataPoints().get(0).getValue();
//                        //System.out.println(dbName + " " + d);
//                        //System.out.println(dbName + " " + m.getDataPoints().size() + " dataPoints");
//                        
//                        stats.addDataSize(getDbMeasurement(m));
//                    } else if (m.getName().equals("DATABASE_INDEX_SIZE")) {
//                        stats.addIndexSize(getDbMeasurement(m));
//                    }
//
//                }
//            }
//        }
//        return stats;
//    }
//
//    public void getMeasurements(String groupId) throws IOException {
//        
//        List<com.mongodb.atlas.model.Process> procs = getProcesses(groupId);
//        System.out.println(String.format("%-30s %10s %10s %-10s %-12s %-12s %-12s %-12s %-12s", 
//                "cluster", "itype", "RAM GB", "IOPS", "dataSizeGB", "indexSizeGB", "max IOPS", "avg IOPS", "95p IOPS"));
//        
//        TreeMap<String, String> results = new TreeMap<String, String>();
//       
//        for (com.mongodb.atlas.model.Process proc : procs) {
//            
//            // TODO option for this
//            if (! proc.getTypeName().equals("REPLICA_PRIMARY")) {
//                continue;
//            }
//            
//            //System.out.println(proc.getId());
//            DatabasesStats stats = getDatabasesStats(groupId, proc.getId());
//            
//            // TODO fix for sharded
//            String clusterName = StringUtils.substringBefore(proc.getReplicaSetName(), "-shard-");
//            //System.out.println(clusterName + " " + proc.getHostname());
//            
//            DescriptiveStatistics diskStats = getDiskStats(groupId, proc.getId());
//            
//            //System.out.println(result.getMeasurements().get(0).getName() + ": p95: " + diskStats.getPercentile(95) + ", avg: " + diskStats.getMean() + ", max: " + diskStats.getMax());
//           
//            
//            Cluster cluster = getCluster(groupId, clusterName);
//            
//            String instanceName = cluster.getProviderSettings().getInstanceSizeName();
//            AWSInstanceSize aws = AWSInstanceSize.findByName(instanceName).get();
//            //System.out.println(aws);
//            
//            //System.out.println(cluster.getName() + ", IOPS: " + cluster.getProviderSettings().getDiskIOPS() + ", " + 
//            //cluster.getProviderSettings().getInstanceSizeName());
//            String display = String.format("%-30s %10s %-8.1f %-10d %-12.1f %-12.1f %-12.1f %-12.1f %-12.1f", 
//                    cluster.getName(), instanceName, aws.getRamSizeGB(), cluster.getProviderSettings().getDiskIOPS(),
//                    stats.getTotalDataSize(), stats.getTotalIndexSize(),
//                    diskStats.getMax(), diskStats.getMean(), diskStats.getPercentile(95)
//            );
//            
//            results.put(cluster.getName(), display);
//            
//        }
//        
//        for (Map.Entry<String, String> entry : results.entrySet()) {
//            String key = entry.getKey();
//            String value = entry.getValue();
//            System.out.println(value);
//        }
//        
//    }

}
