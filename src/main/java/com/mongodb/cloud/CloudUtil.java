package com.mongodb.cloud;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.atlas.model.Database;
import com.mongodb.atlas.model.DatabasesResult;
import com.mongodb.atlas.model.DatabasesStats;
import com.mongodb.atlas.model.Measurement;
import com.mongodb.atlas.model.MeasurementDataPoint;
import com.mongodb.atlas.model.MeasurementsResult;
import com.mongodb.atlas.model.ProcessesResult;
import com.mongodb.cloud.model.Cluster;
import com.mongodb.cloud.model.ClustersResult;
import com.mongodb.cloud.model.Disk;
import com.mongodb.cloud.model.DisksResult;
import com.mongodb.cloud.model.Host;
import com.mongodb.cloud.model.HostsResult;
import com.mongodb.util.ByteSizesUtil;

import retrofit2.Call;
import retrofit2.Response;

public class CloudUtil {
    
    private static Logger logger = LoggerFactory.getLogger(CloudUtil.class);

    private AtlasApi service;
    
    private CodecRegistry pojoCodecRegistry;
    private MongoClient mongoClient;
    private String mongoUri;
    
    
    private DescriptiveStatistics diskStats = new DescriptiveStatistics();

    public CloudUtil(String username, String apiKey, String mongoUri) {
        service = CloudServiceGenerator.createService(AtlasApi.class, username, apiKey);
        this.mongoUri = mongoUri;
        init();
    }
    
    private void init() {
        pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        //MongoClientURI source = new MongoClientURI(mongoUri);

//        mongoClient = new MongoClient(source);
//        List<ServerAddress> addrs = mongoClient.getServerAddressList();
//        mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
//        logger.debug("Connected to source");
    }

    public List<Cluster> getClusters(String groupId) throws IOException {

        Call<ClustersResult> callSync = service.getClusters(groupId);
        Response<ClustersResult> response = callSync.execute();
        //System.out.println(response);
        ClustersResult clusters = response.body();
        return clusters.getClusters();
    }
    
    public List<Host> getHosts(String groupId) throws IOException {

        Call<HostsResult> callSync = service.getHosts(groupId);
        Response<HostsResult> response = callSync.execute();
        //System.out.println(response);
        HostsResult result = response.body();
        return result.getHosts();
    }
    
    public List<Disk> getDisks(String groupId, String hostId) throws IOException {

        Call<DisksResult> callSync = service.getDisks(groupId, hostId);
        Response<DisksResult> response = callSync.execute();
        //System.out.println(response);
        DisksResult result = response.body();
        return result.getDisks();
    }
    
    public Cluster getCluster(String groupId, String clusterName) throws IOException {

        Call<Cluster> callSync = service.getCluster(groupId, clusterName);
        Response<Cluster> response = callSync.execute();
        //System.out.println(response);
        Cluster cluster = response.body();
        return cluster;
    }

    public List<com.mongodb.atlas.model.Process> getProcesses(String groupId) throws IOException {

        Call<ProcessesResult> callSync = service.getProcesses(groupId);
        Response<ProcessesResult> response = callSync.execute();
        System.out.println(response);
        ProcessesResult procs = response.body();

        return procs.getProcesses();
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
                        System.out.println(m.getName() + " " + d);
                    }
                    
                }
            }
        }
    }
    
    public List<Measurement> getDiskMeasurements(String groupId, String hostId, String paritionName) throws IOException {
        Call<MeasurementsResult> callSync = service.getDiskMeasurements(groupId, hostId, paritionName);
        Response<MeasurementsResult> response = callSync.execute();
        MeasurementsResult result = response.body();
        if (result != null) {
            return result.getMeasurements();
        }
        return null;
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
    
    public static double bytesToGigabytes(final double pV) {
        return (pV * 9.31323e-10d);
    }
    
    private double getDbMeasurement(Measurement m) {
        double total = 0.0;
        for (MeasurementDataPoint d : m.getDataPoints()) {
            if (d.getValue() != null) {
                if (!m.getUnits().equals("BYTES")) {
                    throw new RuntimeException("Unexpected unit: " + m.getUnits());
                }
                //System.out.println(d.getValue());
                double gb = bytesToGigabytes(d.getValue());
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
        
        List<Host> hosts = getHosts(groupId);
        for (Host host : hosts) {
            //System.out.println(host);
            List<Disk> disks = getDisks(groupId, host.getId());
            for (Disk disk : disks) {
                
                List<Measurement> measurements = this.getDiskMeasurements(groupId, host.getId(), disk.getPartitionName());
                for (Measurement m : measurements) {
                    if (m.getName().equals("DISK_PARTITION_SPACE_FREE")) {
                        List<MeasurementDataPoint> dps = m.getDataPoints();
                        
                        MeasurementDataPoint firstPoint = dps.get(0);
                        double firstGB = ByteSizesUtil.bytesToGigabytes(firstPoint.getValue());
                        
                        MeasurementDataPoint lastPoint = dps.get(dps.size()-1);
                        double lastGB = ByteSizesUtil.bytesToGigabytes(lastPoint.getValue());
                        
                        double delta = firstGB - lastGB;
                        
                        double perDay = delta / 2;
                        double days = lastGB/perDay;
                        
                        System.out.println(host.getHostname() + "    " + disk.getPartitionName() + " " + lastGB + " " + delta + " " + days);
                    }
                }
            }
        }
        

    }

}
