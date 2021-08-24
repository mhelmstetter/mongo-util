package com.mongodb.atlas;

import com.mongodb.atlas.model.Cluster;
import com.mongodb.atlas.model.ClustersResult;
import com.mongodb.atlas.model.DatabasesResult;
import com.mongodb.atlas.model.LogCollectionJob;
import com.mongodb.atlas.model.LogCollectionJobRequest;
import com.mongodb.atlas.model.MeasurementsResult;
import com.mongodb.atlas.model.ProcessesResult;
import com.mongodb.atlas.model.ProjectsResult;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface AtlasApi {
    
	@GET("groups")
    Call<ProjectsResult> getProjects();
	
    @GET("groups/{groupId}/clusters")
    Call<ClustersResult> getClusters(@Path("groupId") String groupId);
    
    @GET("groups/{groupId}/clusters/{clusterName}")
    Call<Cluster> getCluster(@Path("groupId") String groupId, @Path("clusterName") String clusterName);
    
    @GET("groups/{groupId}/processes")
    Call<ProcessesResult> getProcesses(@Path("groupId") String groupId);
    
    @GET("groups/{groupId}/processes")
    Call<ProcessesResult> getProcesses(@Path("groupId") String groupId, @Query("pageNum") Integer pageNum);
    
    @GET("groups/{groupId}/processes/{hostId}/measurements?granularity=PT1M&period=P2D&&m=CACHE_BYTES_READ_INTO&m=CACHE_BYTES_WRITTEN_FROM")
    Call<MeasurementsResult> getMeasurements(@Path("groupId") String groupId, @Path("hostId") String hostId);
    
    @GET("groups/{groupId}/processes/{hostId}/disks/xbdb/measurements?granularity=PT1M&period=P2D&m=DISK_PARTITION_IOPS_TOTAL")
    Call<MeasurementsResult> getDiskMeasurements(@Path("groupId") String groupId, @Path("hostId") String hostId);
    
    
    //GET api/atlas/v1.0/groups/{GROUP-ID}/processes/{HOST}:{PORT}/databases
    @GET("groups/{groupId}/processes/{hostId}/databases")
    Call<DatabasesResult> getDatabases(@Path("groupId") String groupId, @Path("hostId") String hostId);
    
    //GET api/atlas/v1.0/groups/{GROUP-ID}/processes/{HOST}:{PORT}/databases/{DATABASE-ID}/measurements
    @GET("groups/{groupId}/processes/{hostId}/databases/{databaseName}/measurements?granularity=PT24H&period=PT24H")
    Call<MeasurementsResult> getDatabaseMeasurements(@Path("groupId") String groupId,
            @Path("hostId") String hostId, @Path("databaseName") String databaseName);
    
    @Headers("Content-Type: application/json")
    @POST("groups/{groupId}/logCollectionJobs")
    Call<LogCollectionJob> startLogCollectionJob(@Path("groupId") String groupId, 
    		@Body LogCollectionJobRequest logCollectionJobRequest);
    
    
//    @GET("groups/{groupId}/clusters/{hostId}/logs/FTDC")
//    Call<ResponseBody> getFtdc(@Path("groupId") String groupId,
//            @Path("hostId") String hostId, @Query("startDate") long startDate);

}
