package com.mongodb.util;

import java.util.Set;
import java.util.TreeSet;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class DiffUtils {
	
	private static Logger logger = LoggerFactory.getLogger(DiffUtils.class);
	
	public static boolean compareHashes(byte[] sourceBytes, byte[] destBytes) {
		String sourceHash = CodecUtils.md5Hex(sourceBytes);
		String destHash = CodecUtils.md5Hex(destBytes);
		return sourceHash.equals(destHash);
	}
	
	/**
	 * This comparison handles the (very) special case that we could have (usually
	 * due to some client/driver bug) 2 documents that differ only by the order of
	 * their fields.
	 * 
	 * @param sourceDoc
	 * @param destDoc
	 * @return
	 */
	public static boolean compareDocuments(String ns, RawBsonDocument sourceDoc, RawBsonDocument destDoc) {
		Object id = sourceDoc.get("_id");
		Set<String> sourceKeys = sourceDoc.keySet();
		Set<String> destKeys = destDoc.keySet();
		Set<String> sortedKeys = new TreeSet<String>();
		boolean setsEqual = sourceKeys.equals(destKeys);
		Set<String> diff = null;
		if (!setsEqual) {
			diff = Sets.difference(sourceKeys, destKeys);
			logger.debug("    - keys do not match: keys missing from source" + diff);

			diff = Sets.difference(destKeys, sourceKeys);
			logger.debug("    - keys do not match: keys missing from dest" + diff);
			sortedKeys.addAll(diff);
		}

		sortedKeys.addAll(sourceKeys);

		BsonDocument sourceDocNew = new BsonDocument();
		BsonDocument destDocNew = new BsonDocument();
		for (String key : sortedKeys) {
			BsonValue sourceVal = sourceDoc.get(key);
			BsonValue destVal = destDoc.get(key);
			boolean valuesEqual = sourceVal != null && destVal != null && sourceVal.equals(destVal);
			if (!valuesEqual) {
				logger.debug(String.format("    - values not equal for key: %s, sourceVal: %s, destVal: %s", key,
						sourceVal, destVal));
			}
			if (sourceVal != null) {
				sourceDocNew.append(key, sourceVal);
			}
			if (destVal != null) {
				destDocNew.append(key, destVal);
			}

			if (setsEqual) {
				RawBsonDocument sourceRawNew = new RawBsonDocument(sourceDocNew, new BsonDocumentCodec());
				RawBsonDocument destRawNew = new RawBsonDocument(destDocNew, new BsonDocumentCodec());
				boolean newDocsMatch = DiffUtils.compareHashes(sourceRawNew.getByteBuffer().array(),
						destRawNew.getByteBuffer().array());
				logger.debug(String.format("%s - bytes match: %s", ns, newDocsMatch));
			}
		}
		return sourceDoc.equals(destDoc);
	}

}
