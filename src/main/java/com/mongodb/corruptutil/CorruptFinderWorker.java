package com.mongodb.corruptutil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.bson.BsonSerializationException;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class CorruptFinderWorker implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(CorruptFinderWorker.class);

    private MongoCollection<RawBsonDocument> collection;
    private MongoClient client;
    FileOutputStream fos = null;
    ZipOutputStream zipStream = null;
    private File outDir;

    public CorruptFinderWorker(MongoClient client, MongoCollection<RawBsonDocument> collection, File outDir) {
        this.collection = collection;
        this.client = client;
        this.outDir = outDir;
    }
    
    private void zip(String path, byte[] bytes) throws IOException {
        if (fos == null) {
            File outFile = new File(outDir, collection.getNamespace().getFullName()+".zip");
            fos = new FileOutputStream(outFile);
            zipStream = new ZipOutputStream(fos);
        }
        ZipEntry zipEntry = new ZipEntry(path);
        zipStream.putNextEntry(zipEntry);
        zipStream.write(bytes, 0, bytes.length);
        zipStream.closeEntry();
    }
    
    private void handleCorrupt(RawBsonDocument doc, BsonValue id) {
        ByteBuffer buffer = doc.getByteBuffer().asNIO();
        try {
            zip(id.toString()+".bson", buffer.array());
        } catch (IOException e) {
            logger.error("Error adding to zip", e);
        }
    }

    @Override
    public void run() {
        MongoCursor<RawBsonDocument> cursor = null;
        long start = System.currentTimeMillis();
        long last = start;
        long count = 0;
        long corruptCount = 0;

        try {
            cursor = collection.find().noCursorTimeout(true).iterator();
            Number total = collection.countDocuments();
            while (cursor.hasNext()) {
                count++;
                RawBsonDocument doc = cursor.next();
                BsonValue id = null;
                try {
                    id = doc.get("_id");
                } catch (Exception e) {
                    corruptCount++;
                    logger.trace(String.format("%s - Error reading doc id, count: %s, error: %s",
                            collection.getNamespace(), count, e));
                    handleCorrupt(doc, id);
                    continue;
                }

                try {
                    String s = doc.toJson();
                } catch (BsonSerializationException bse) {
                    corruptCount++;
                    logger.trace(String.format("%s - Error serializing doc with _id: %s, count: %s, error: %s",
                            collection.getNamespace(), id, count, bse));
                    handleCorrupt(doc, id);
                    continue;
                }

                long current = System.currentTimeMillis();
                long delta = (current - last) / 1000;
                if (delta >= 30) {
                    logger.debug(String.format("%s - checked %s / %s documents, corruptCount: %s",
                            collection.getNamespace(), count, total));
                    last = current;
                }
            }

        } finally {
            cursor.close();
            if (fos != null) {
                try {
                    zipStream.close();
                    fos.close();
                } catch (IOException e) {
                    logger.debug("Error closing zip file", e);
                }
                
            }
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start) / 1000.0;
        logger.debug(String.format("Done validating %s, %s documents in %f seconds, corruptCount: %s",
                collection.getNamespace(), count, dur, corruptCount));

    }

}
