package com.mongodb.mongosync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.ShardTimestamp;
import com.mongodb.util.AtomicFileWriter;

public class TimestampFile {
	
	protected static final Logger logger = LoggerFactory.getLogger(TimestampFile.class);
	
	private String shardId;
	private File tsFile;
	
	
	public TimestampFile(String shardId) {
		this.shardId = shardId;
		String fileName = shardId + ".timestamp";
		tsFile = new File(fileName);
	}
	
	public ShardTimestamp getShardTimestamp() throws IOException {
		BufferedReader br = null;
		String shardName = null;
		String tsStr = null;
		
		try {
			br = new BufferedReader(new FileReader(tsFile));
	    	shardName = br.readLine();
	    	tsStr = br.readLine();
		} finally {
			br.close();
		}
		
    	if (shardName == null || tsStr == null) {
    		throw new IOException("Invalid or empty timestamp file");
    	}
    	
    	long tsLong = Long.parseLong(tsStr);
    	BsonTimestamp ts = new BsonTimestamp(tsLong);
    	return new ShardTimestamp(shardName, ts);
    }


	public boolean exists() {
		return tsFile.exists();
	}


	@Override
	public String toString() {
		return tsFile.getName();
	}

	public void update(ShardTimestamp shardTimestamp) throws IOException {
		update(shardTimestamp.getTimestamp());
	}

	public void update(BsonTimestamp ts) throws IOException {
		if (ts == null) {
			return;
		}
		if (! tsFile.exists()) {
			boolean success = tsFile.createNewFile();
		}
		Path tsFilePath = Paths.get(tsFile.getAbsolutePath());
		
		try (AtomicFileWriter cw = new AtomicFileWriter(tsFilePath)) {
            try {
            	cw.write(shardId);
                cw.write('\n');
                cw.write(String.valueOf(ts.getValue()));
                cw.commit();
            } finally {
                cw.abort();
            }
        } catch (IOException ioe) {
        	logger.error(String.format("timestamp file update error: %s", tsFile), ioe);
        }
	}

	public String getShardId() {
		return shardId;
	}

}
