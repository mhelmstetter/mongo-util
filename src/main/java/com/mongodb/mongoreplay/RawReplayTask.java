package com.mongodb.mongoreplay;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.bson.BSONObject;
import org.bson.BsonBinaryReader;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.io.ByteBufferBsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.util.ShapeUtil;

public class RawReplayTask implements Callable<ReplayResult> {

    // private TimedEvent event;
    private Monitor monitor;
    private MongoClient mongoClient;
    private Document commandDoc;
    private String databaseName;
    private String collectionName;
    private Command command;
    private String queryShape;
    
    private ReplayOptions replayOptions;
    
    private BSONObject raw;
    private boolean ignore = false;
    
    private final static DocumentCodec documentCodec = new DocumentCodec();
    private final static DecoderContext decoderContext = DecoderContext.builder().build();

    protected static final Logger logger = LoggerFactory.getLogger(RawReplayTask.class);

    public RawReplayTask(Monitor monitor, MongoClient mongoClient, ReplayOptions replayOptions, BSONObject raw) {
        this.monitor = monitor;
        this.mongoClient = mongoClient;
        this.replayOptions = replayOptions;
        this.raw = raw;
    }
    
    private void process() {
        byte[] bytes = (byte[]) raw.get("body");

        if (bytes.length == 0) {
            return;
        }

        BSONObject header = (BSONObject) raw.get("header");

        if (header != null) {
            int opcode = (Integer) header.get("opcode");
            ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
            BsonBinaryReader reader = new BsonBinaryReader(bsonInput);

            int messageLength = bsonInput.readInt32();
            int requestId = bsonInput.readInt32();
            int responseTo = bsonInput.readInt32();
            int headerOpcode = bsonInput.readInt32();

            if (opcode == 2004) {
                int flags = bsonInput.readInt32();
                String collectionName = bsonInput.readCString();
                databaseName = StringUtils.substringBefore(collectionName, ".$cmd");
                if (databaseName.equals("local") || databaseName.equals("admin")) {
                    return;
                }
                if (replayOptions.getIgnoredCollections().contains(collectionName)) {
                    return;
                }
                
                int nskip = bsonInput.readInt32();
                int nreturn = bsonInput.readInt32();
                
                this.commandDoc = documentCodec.decode(reader, decoderContext);
                processCommand(databaseName);
                //written++;

            } else if (opcode == 2010) {
                int p1 = bsonInput.getPosition();
                databaseName = bsonInput.readCString();
                if (databaseName.equals("local") || databaseName.equals("admin")) {
                    return;
                }
                String command = bsonInput.readCString();
                this.commandDoc = documentCodec.decode(reader, decoderContext);
                commandDoc.remove("shardVersion");
                processCommand(databaseName);
            } else if (opcode == 2013) {  // OP_MSG
                int flags = bsonInput.readInt32();
                boolean moreSections = true;
                while (moreSections) {
                    byte kindByte = bsonInput.readByte();
                    
                    if (kindByte == 0) {
                        commandDoc = documentCodec.decode(reader, decoderContext);
                        
                        moreSections = messageLength > bsonInput.getPosition();
                        
                        databaseName = commandDoc.getString("$db");
                        if (databaseName.equals("local") || databaseName.equals("admin")) {
                            continue;
                        }
                        
                        commandDoc.remove("lsid");
                        commandDoc.remove("$db");
                        commandDoc.remove("$readPreference");
                        
                        if (! moreSections) {
//                            if (commandDoc.containsKey("count")) {
//                                System.out.println();
//                            }
                            processCommand(databaseName);
                        }
                        
                    } else {
                        //logger.warn("ignored OP_MSG having Section kind 1");
                        //ignored++;
                        int p0 = bsonInput.getPosition();
                        int size = bsonInput.readInt32();
                        String seq = bsonInput.readCString();
                        int p1 = bsonInput.getPosition();
                        int remaining = size - (p1 - p0);
                        
                        byte[] mb = new byte[remaining];
                        
                        bsonInput.readBytes(mb);
                        
                        BsonBinaryReader r2 = new BsonBinaryReader(ByteBuffer.wrap(mb));
                        Document d1 = documentCodec.decode(r2, decoderContext);
                        
                        if (commandDoc != null && commandDoc.containsKey("insert")) {
                            commandDoc.put("documents", Arrays.asList(d1));
                            processCommand(databaseName);
                        } else if (commandDoc != null && commandDoc.containsKey("update")) {
                            commandDoc.put("updates", Arrays.asList(d1));
                            processCommand(databaseName);
                        } else if (commandDoc != null && commandDoc.containsKey("delete")) {
                            commandDoc.put("deletes", Arrays.asList(d1));
                            processCommand(databaseName);
                        } else {
                            logger.debug("wtf: " + commandDoc);
                        }
                        
                        moreSections = messageLength > bsonInput.getPosition();
                    }
                }
                    
            } else {
                logger.warn("ignored opcode: " + opcode);
                //ignored++;
                ignore = true;
            }
        }
    }
    
    private void sleep() {
    	Long sleep = replayOptions.getSleepMillis();
    	if (sleep != null) {
    		try {
				Thread.currentThread().sleep(sleep);
			} catch (InterruptedException e) {
			}
    	}
    }
    
    private void processCommand(String databaseName) {
        //System.out.println(commandDoc);
        Set<String> shape = null;
        if (commandDoc.containsKey("$query")) {
            Document queryDoc = (Document)commandDoc.get("$query");
            commandDoc = queryDoc;
        }
// do we need to unwrap here? one case is count() which should not
// be unwrapped. What are the other cases if any?
//        else if (commandDoc.containsKey("query")) {
//            Document queryDoc = (Document)commandDoc.get("query");
//            commandDoc = queryDoc;
//        }
        
        if (commandDoc.containsKey("find")) {
            command = Command.FIND;
            collectionName = commandDoc.getString("find");
            Document predicates = (Document) commandDoc.get("filter");
            shape = ShapeUtil.getShape(predicates);
        }  else if (commandDoc.containsKey("insert")) {
            command = Command.INSERT;
            collectionName = commandDoc.getString("insert");
        }  else if (commandDoc.containsKey("update")) {
            command = Command.UPDATE;
            collectionName = commandDoc.getString("update");
            List<Document> updates = (List<Document>)commandDoc.get("updates");
            for (Document updateDoc : updates) {
                Document query = (Document)updateDoc.get("q");
                shape = ShapeUtil.getShape(query);
                if (replayOptions.getRemoveUpdateFields() != null) {
                    for (String fieldName : replayOptions.getRemoveUpdateFields()) {
                        query.remove(fieldName);
                    }
                }
            }
        }  else if (commandDoc.containsKey("getMore")) {
            command = Command.GETMORE;
            //getMoreCount++;
            //ignored++;
            ignore = true;
            return;
        }  else if (commandDoc.containsKey("aggregate")) {
            command = Command.AGGREGATE;
            List<Document> stages = (List<Document>)commandDoc.get("pipeline");
            if (stages != null) {
                for (Document stage : stages) {
                    // this will actually crash mongod on OSX
                    if (stage.containsKey("$mergeCursors")) {
                        //ignored++;
                        return;
                    }
                }
                
            }
            commandDoc.remove("fromRouter");
        } else if (commandDoc.containsKey("delete")) {
            command = Command.DELETE;
            collectionName = commandDoc.getString("delete");
        } else if (commandDoc.containsKey("count")) {
            command = Command.COUNT;
            collectionName = commandDoc.getString("count");
        } else if (commandDoc.containsKey("findandmodify")) {
            command = Command.FIND_AND_MODIFY;
            collectionName = commandDoc.getString("findandmodify");
        } else {
            //logger.warn("ignored command: " + commandDoc);
            //ignored++;
            ignore = true;
            return;
        }
        
        if (replayOptions.getIgnoredCollections().contains(collectionName)) {
            //ignored++;
            ignore = true;
            return;
        }
        
        if (shape != null) {
            queryShape = shape.toString();
        }
    }

    @Override
    public ReplayResult call() {
        
        process();
        if (command == null || ignore) {
            return null;
        }

        // event = new TimedEvent();
        long start = System.nanoTime();
        ReplayResult replayResult = null;
        String db = null;
        if (replayOptions.getDbNamesMap() != null) {
        	db = replayOptions.getDbNamesMap().get(databaseName);
        	if (db == null) {
        		db = databaseName;
        	}
        } else {
        	db = databaseName;
        }
        try {
            Document commandResult = null;
            if (command.isRead()) {
                 
                if (replayOptions.getReadConcern() != null) {
                    commandDoc.put("readConcern", replayOptions.getReadConcern());
                }
                commandResult = mongoClient.getDatabase(db).runCommand(commandDoc, mongoClient.getReadPreference());
                 
            } else {
                
                commandDoc.put("writeConcern", replayOptions.getWriteConcern());
                commandResult = mongoClient.getDatabase(db).runCommand(commandDoc);
            }
            long duration = System.nanoTime() - start;
            // long duration = event.stop();
            Number ok = (Number) commandResult.get("ok");
            // logger.debug("result: " + result);
            if (ok.equals(1.0)) {
                monitor.incrementEventCount();
                replayResult = new ReplayResult(queryShape, db, collectionName, command, duration, true);
            } else {
                // event.incrementError(1);
                replayResult = new ReplayResult(queryShape, db, collectionName, command, duration, false);
                monitor.incrementErrorCount();
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error executing task", e);
            monitor.incrementErrorCount();
        }

        // monitor.add(event);
        return replayResult;
    }
}
