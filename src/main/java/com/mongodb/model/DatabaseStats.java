package com.mongodb.model;

import org.bson.Document;

public class DatabaseStats {
	

	private int indexes;
	
	private Double avgObjSize;
	
	private Double dataSize;
	private Double storageSize;
	private Double indexSize;
	private Double scaleFactor;
	
	private Long documentCount;
	
	
	
	public static DatabaseStats fromDocument(Document doc) {
		
		// collections=1, views=0, objects=2, avgObjSize=109.0, dataSize=218.0, storageSize=36864.0, numExtents=0, indexes=1, indexSize=36864.0, //
		// scaleFactor=1.0, fsUsedSize=4.66940358656E11, fsTotalSize=4.99963174912E11, ok=1.0}}, shard_A/localhost:28107,localhost:28108=Document{{db=admin, collections=1, views=0, objects=4, avgObjSize=146.25, dataSize=585.0, storageSize=36864.0, numExtents=0, indexes=1, indexSize=36864.0, scaleFactor=1.0, fsUsedSize=4.66940358656E11, fsTotalSize=4.99963174912E11, ok=1.0}}}}, objects=6, avgObjSize=133.66666666666666, dataSize=803, storageSize=73728, numExtents=0, indexes=2, indexSize=73728, scaleFactor=1, fileSize=0, ok=1.0, operationTime=Timestamp{value=7160988140713279489, seconds=1667297478, inc=1}, $clusterTime=Document{{clusterTime=Timestamp{value=7160988140713279489, seconds=1667297478, inc=1}, 
		// signature=Document{{hash=org.bson.types.Binary@c98f581, keyId=0}}}}}}
		DatabaseStats stats = new DatabaseStats();
		
		stats.documentCount = getLong(doc, "objects");
		stats.indexes = getInteger(doc, "indexes");
		stats.avgObjSize = getDouble(doc, "avgObjSize");

		stats.dataSize = getDouble(doc, "dataSize");
		stats.storageSize = getDouble(doc, "storageSize");
		stats.indexSize = getDouble(doc, "indexSize");
		
		return stats;
	}
	
	private static Double getDouble(Document doc, String key) {
		Object val = doc.get(key);
		if (val != null) {
			if (val instanceof Number) {
				return ((Number)val).doubleValue();
			} else {
				throw new IllegalArgumentException(String.format("Unexpected type %s, expected Number", val.getClass().getName()));
			}
		}
		return null;
	}
	
	private static Integer getInteger(Document doc, String key) {
		Object val = doc.get(key);
		if (val != null) {
			if (val instanceof Number) {
				return ((Number)val).intValue();
			} else {
				throw new IllegalArgumentException(String.format("Unexpected type %s, expected Number", val.getClass().getName()));
			}
		}
		return null;
	}
	
	private static Long getLong(Document doc, String key) {
		Object val = doc.get(key);
		if (val != null) {
			if (val instanceof Number) {
				return ((Number)val).longValue();
			} else {
				throw new IllegalArgumentException(String.format("Unexpected type %s, expected Number", val.getClass().getName()));
			}
		}
		return null;
	}

	public int getIndexes() {
		return indexes;
	}

	public Double getAvgObjSize() {
		return avgObjSize;
	}

	public Double getDataSize() {
		return dataSize;
	}

	public Double getStorageSize() {
		return storageSize;
	}

	public Double getIndexSize() {
		return indexSize;
	}

	public Double getScaleFactor() {
		return scaleFactor;
	}

	public Long getDocumentCount() {
		return documentCount;
	}

}
