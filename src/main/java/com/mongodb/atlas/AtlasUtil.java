package com.mongodb.atlas;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.atlas.model.AWSInstanceSize;
import com.mongodb.atlas.model.Cluster;
import com.mongodb.atlas.model.ClustersResult;
import com.mongodb.atlas.model.Database;
import com.mongodb.atlas.model.DatabasesResult;
import com.mongodb.atlas.model.DatabasesStats;
import com.mongodb.atlas.model.LogCollectionJob;
import com.mongodb.atlas.model.LogCollectionJobRequest;
import com.mongodb.atlas.model.Measurement;
import com.mongodb.atlas.model.MeasurementDataPoint;
import com.mongodb.atlas.model.MeasurementsResult;
import com.mongodb.atlas.model.ProcessesResult;
import com.mongodb.atlas.model.Project;
import com.mongodb.atlas.model.ProjectsResult;
import com.mongodb.client.MongoClient;
import com.mongodb.util.ByteSizesUtil;

import retrofit2.Call;
import retrofit2.Response;

public class AtlasUtil {
	
	private final static String[] FTDC_LOGS = {"FTDC"};
    
    private static Logger logger = LoggerFactory.getLogger(AtlasUtil.class);

    private AtlasApi service;
    
    private CodecRegistry pojoCodecRegistry;
    private MongoClient mongoClient;
    private String mongoUri;
    
    
    private DescriptiveStatistics diskStats = new DescriptiveStatistics();

    public AtlasUtil(String username, String apiKey, String mongoUri) {
        service = AtlasServiceGenerator.createService(AtlasApi.class, username, apiKey);
        this.mongoUri = mongoUri;
        init();
    }
    
    private void init() {
    	pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        ConnectionString source = new ConnectionString(mongoUri);
    }
    
    public List<Project> getProjects() throws IOException {
    	Call<ProjectsResult> callSync = service.getProjects();
    	Response<ProjectsResult> response = callSync.execute();
    	ProjectsResult projects = response.body();
    	return projects.getProjects();
    }

    public List<Cluster> getClusters(String groupId) throws IOException {

        Call<ClustersResult> callSync = service.getClusters(groupId);
        Response<ClustersResult> response = callSync.execute();
        ClustersResult clusters = response.body();
        return clusters.getClusters();
    }
    
    public Cluster getCluster(String groupId, String clusterName) throws IOException {

        Call<Cluster> callSync = service.getCluster(groupId, clusterName);
        Response<Cluster> response = callSync.execute();
        Cluster cluster = response.body();
        return cluster;
    }

    public List<com.mongodb.atlas.model.Process> getProcesses(String groupId) throws IOException {

        Call<ProcessesResult> callSync = service.getProcesses(groupId);
        
        
        Response<ProcessesResult> response = callSync.execute();
        
        ProcessesResult procs = response.body();
        
        int total = procs.getTotalCount();
        
        List<com.mongodb.atlas.model.Process> procList = procs.getProcesses();
        if (total > procList.size()) {
            List<com.mongodb.atlas.model.Process> result = new ArrayList<com.mongodb.atlas.model.Process>();
            result.addAll(procList);
            int processed = procList.size();
            int pageNum = 2;
            while (processed < total) {
                callSync = service.getProcesses(groupId, pageNum++);
                response = callSync.execute();
                procs = response.body();
                procList = procs.getProcesses();
                processed += procList.size();
                result.addAll(procList);
            }
            return result;
            
        } else {
            return procList; 
        }
        
    }
    
    public void getLogs(String groupId, String hostId, long startDate) throws IOException {
    	LogCollectionJobRequest req = new LogCollectionJobRequest();
    	req.setResourceType("REPLICASET");
    	req.setResourceName(hostId);
    	req.setLogTypes(FTDC_LOGS);
    	Call<LogCollectionJob> logCall = service.startLogCollectionJob(groupId, req);
    	Response<LogCollectionJob> logResponse = logCall.execute();
    	LogCollectionJob job =  logResponse.body();
    	
//    	Call<ResponseBody> callSync = service.getFtdc(groupId, hostId, startDate);
//        Response<ResponseBody> response = callSync.execute();
//        ResponseBody body = response.body();
//        File file = new File(hostId);
//        InputStream inputStream = null;
//        OutputStream outputStream = null;
//
//        try {
//            byte[] fileReader = new byte[4096];
//
//            long fileSize = body.contentLength();
//            long fileSizeDownloaded = 0;
//
//            inputStream = body.byteStream();
//            outputStream = new FileOutputStream(file);
//
//            while (true) {
//                int read = inputStream.read(fileReader);
//
//                if (read == -1) {
//                    break;
//                }
//
//                outputStream.write(fileReader, 0, read);
//
//                fileSizeDownloaded += read;
//
//                logger.debug("file download: " + fileSizeDownloaded + " of " + fileSize);
//            }
//
//            outputStream.flush();
//        } catch (IOException e) {
//        	logger.error("Error processing FTDC download", e);
//        } finally {
//            if (inputStream != null) {
//                inputStream.close();
//            }
//
//            if (outputStream != null) {
//                outputStream.close();
//            }
//            if (body != null) {
//            	body.close();
//            }
//        }
    }

    public void getMeasurements(String groupId, String hostId) throws IOException {
        Call<MeasurementsResult> callSync = service.getMeasurements(groupId, hostId);
        Response<MeasurementsResult> response = callSync.execute();
        MeasurementsResult result = response.body();
        if (result != null) {
            List<Measurement> measurements = result.getMeasurements();
            for (Measurement m : measurements) {
                for (MeasurementDataPoint d : m.getDataPoints()) {
                    if (d.getValue() != null) {
                        System.out.println(d);
                    }
                    
                }
            }
        }
    }
    
    public DescriptiveStatistics getDiskStats(String groupId, String processId) throws IOException {
        diskStats.clear();
        Call<MeasurementsResult> callSync = service.getDiskMeasurements(groupId, processId);
        Response<MeasurementsResult> response = callSync.execute();
        MeasurementsResult result = response.body();
        if (result != null) {
            List<Measurement> measurements = result.getMeasurements();
            for (Measurement m : measurements) {
                for (MeasurementDataPoint d : m.getDataPoints()) {
                    if (d.getValue() != null) {
                        //System.out.println(d);
                        diskStats.addValue(d.getValue());
                    }
                }
            }
        }
        return diskStats;
    }
    
    public List<String> getDatabaseNames(String groupId, String processId) throws IOException {
        ArrayList<String> results = new ArrayList<String>();
        Call<DatabasesResult> callSync = service.getDatabases(groupId, processId);
        Response<DatabasesResult> response = callSync.execute();
        DatabasesResult result = response.body();
        if (result != null) {
            for (Database db : result.getDatabases()) {
                if (db.getDatabaseName().equals("local")) {
                    continue;
                }
                //System.out.println("    " + db.getDatabaseName());
                results.add(db.getDatabaseName());
            }
        }
        return results;
    }
    
    private double getDbMeasurement(Measurement m) {
        double total = 0.0;
        for (MeasurementDataPoint d : m.getDataPoints()) {
            if (d.getValue() != null) {
                if (!m.getUnits().equals("BYTES")) {
                    throw new RuntimeException("Unexpected unit: " + m.getUnits());
                }
                //System.out.println(d.getValue());
                double gb = ByteSizesUtil.bytesToGigabytes(d.getValue());
                total += gb;
            } else {
                throw new RuntimeException("null dataPoint, WTF!?");
            }
        }
        return total;
    }
    
    public DatabasesStats getDatabasesStats(String groupId, String hostId) throws IOException {
        DatabasesStats stats = new DatabasesStats();
        List<String> databases = getDatabaseNames(groupId, hostId);
        for (String dbName : databases) {
            Call<MeasurementsResult> callSync = service.getDatabaseMeasurements(groupId, hostId, dbName);
            Response<MeasurementsResult> response = callSync.execute();
            MeasurementsResult result = response.body();
            if (result != null) {
                List<Measurement> measurements = result.getMeasurements();
                for (Measurement m : measurements) {
                    // System.out.println(m.getName());
                    if (m.getName().equals("DATABASE_DATA_SIZE")) {
                        //double d = m.getDataPoints().get(0).getValue();
                        //System.out.println(dbName + " " + d);
                        //System.out.println(dbName + " " + m.getDataPoints().size() + " dataPoints");
                        
                        stats.addDataSize(getDbMeasurement(m));
                    } else if (m.getName().equals("DATABASE_INDEX_SIZE")) {
                        stats.addIndexSize(getDbMeasurement(m));
                    }

                }
            }
        }
        return stats;
    }

    public void getMeasurements(String groupId) throws IOException {
        
        List<com.mongodb.atlas.model.Process> procs = getProcesses(groupId);
        System.out.println(String.format("%-30s %10s %10s %-10s %-12s %-12s %-12s %-12s %-12s", 
                "cluster", "itype", "RAM GB", "IOPS", "dataSizeGB", "indexSizeGB", "max IOPS", "avg IOPS", "95p IOPS"));
        
        TreeMap<String, String> results = new TreeMap<String, String>();
       
        for (com.mongodb.atlas.model.Process proc : procs) {
            
            // TODO option for this
            if (! proc.getTypeName().equals("REPLICA_PRIMARY")) {
                continue;
            }
            
            //System.out.println(proc.getId());
            DatabasesStats stats = getDatabasesStats(groupId, proc.getId());
            
            // TODO fix for sharded
            String clusterName = StringUtils.substringBefore(proc.getReplicaSetName(), "-shard-");
            //System.out.println(clusterName + " " + proc.getHostname());
            
            DescriptiveStatistics diskStats = getDiskStats(groupId, proc.getId());
            
            //System.out.println(result.getMeasurements().get(0).getName() + ": p95: " + diskStats.getPercentile(95) + ", avg: " + diskStats.getMean() + ", max: " + diskStats.getMax());
           
            
            Cluster cluster = getCluster(groupId, clusterName);
            
            String instanceName = cluster.getProviderSettings().getInstanceSizeName();
            AWSInstanceSize aws = AWSInstanceSize.findByName(instanceName).get();
            //System.out.println(aws);
            
            //System.out.println(cluster.getName() + ", IOPS: " + cluster.getProviderSettings().getDiskIOPS() + ", " + 
            //cluster.getProviderSettings().getInstanceSizeName());
            String display = String.format("%-30s %10s %-8.1f %-10d %-12.1f %-12.1f %-12.1f %-12.1f %-12.1f", 
                    cluster.getName(), instanceName, aws.getRamSizeGB(), cluster.getProviderSettings().getDiskIOPS(),
                    stats.getTotalDataSize(), stats.getTotalIndexSize(),
                    diskStats.getMax(), diskStats.getMean(), diskStats.getPercentile(95)
            );
            
            results.put(cluster.getName(), display);
            
        }
        
        for (Map.Entry<String, String> entry : results.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            System.out.println(value);
        }
        
    }

}
