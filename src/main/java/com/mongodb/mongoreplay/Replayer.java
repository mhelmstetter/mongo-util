package com.mongodb.mongoreplay;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.bson.BSONObject;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.io.ByteBufferBsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.WriteConcern;
import com.mongodb.util.ShapeUtil;

import static java.util.Collections.singletonList;

public class Replayer {

    // private TimedEvent event;
    private Monitor monitor;
    private MongoClient mongoClient;
    //private Document commandDoc;
//    private String databaseName;
//    private String collectionName;
//    private Command command;
//    private String queryShape;
    
    private ReplayOptions replayOptions;
    
    //private boolean ignore = false;
    
    private final static DocumentCodec documentCodec = new DocumentCodec();
    private final static DecoderContext decoderContext = DecoderContext.builder().build();

    protected static final Logger logger = LoggerFactory.getLogger(Replayer.class);
    
    private final static BsonDocument DEFAULT_WRITE_CONCERN = WriteConcern.W1.asDocument();

    public Replayer(Monitor monitor, MongoClient mongoClient, ReplayOptions replayOptions) {
        this.monitor = monitor;
        this.mongoClient = mongoClient;
        this.replayOptions = replayOptions;
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
    
    class CommandResult {
    	Set<String> shape = null;
        Command command = null;
        String collectionName = null;
        int ignored = 0;
        boolean ignore = false;
    }
    
    private CommandResult processCommand(String databaseName, Document commandDoc) {
        //System.out.println(commandDoc);
    	CommandResult commandResult = new CommandResult();
        
        
//        if (commandDoc.containsKey("writeConcern")) {
//        	Document wc = (Document)commandDoc.get("writeConcern");
//        	//System.out.println("wc: " + wc);
//        }
        
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
        
        commandDoc.remove("shardVersion");
        commandDoc.remove("$clusterTime");
        commandDoc.remove("$configServerState");
        
        
        if (commandDoc.containsKey("find")) {
            commandResult.command = Command.FIND;
            commandResult.collectionName = commandDoc.getString("find");
            
            Document predicates = (Document) commandDoc.get("filter");
            commandResult.shape = ShapeUtil.getShape(predicates);
        }  else if (commandDoc.containsKey("insert")) {
        	commandResult.command = Command.INSERT;
        	commandResult.collectionName = commandDoc.getString("insert");
            
        }  else if (commandDoc.containsKey("findAndModify")) {
        	 commandResult.command = Command.FIND_AND_MODIFY;
        	 commandResult.collectionName = commandDoc.getString("findAndModify");
        	
        } else if (commandDoc.containsKey("findandmodify")) {
        	commandResult.command = Command.FIND_AND_MODIFY;
        	commandResult.collectionName = commandDoc.getString("findandmodify");
       	
        } else if (commandDoc.containsKey("update")) {
        	commandResult.command = Command.UPDATE;
            Object update = commandDoc.get("update");
            if (update instanceof Document) {
            	
            	Document doc = (Document)update;
            	System.out.println(doc);
            	commandResult.collectionName = commandDoc.getString("update");
            } else if (update instanceof String) {
            	commandResult.collectionName = (String)update;
            } else {
            	logger.debug("Unexepcted update value: " + update);
            }
            
            List<Document> updates = (List<Document>)commandDoc.get("updates");
            if (updates == null) {
            	System.out.println("wtf");
            	return null;
            }
            for (Document updateDoc : updates) {
                Document query = (Document)updateDoc.get("q");
                commandResult.shape = ShapeUtil.getShape(query);
                if (replayOptions.getRemoveUpdateFields() != null) {
                    for (String fieldName : replayOptions.getRemoveUpdateFields()) {
                        query.remove(fieldName);
                    }
                }
            }
        } else if (commandDoc.containsKey("getMore")) {
        	commandResult.command = Command.GETMORE;
            //getMoreCount++;
            //ignored++;
        	commandResult.ignore = true;
            return null;
        }  else if (commandDoc.containsKey("aggregate")) {
        	commandResult.command = Command.AGGREGATE;
            List<Document> stages = (List<Document>)commandDoc.get("pipeline");
            if (stages != null) {
                for (Document stage : stages) {
                    // this will actually crash mongod on OSX
                    if (stage.containsKey("$mergeCursors")) {
                        //ignored++;
                    	commandResult.ignore = true;
                        return null;
                    }
                }
                
            }
            commandDoc.remove("fromRouter");
        } else if (commandDoc.containsKey("delete")) {
        	commandResult.command = Command.DELETE;
        	commandResult.collectionName = commandDoc.getString("delete");
        } else if (commandDoc.containsKey("count")) {
        	commandResult.command = Command.COUNT;
        	commandResult.collectionName = commandDoc.getString("count");
        } else {
            logger.warn("ignored command: " + commandDoc);
            commandResult.ignored++;
            commandResult. ignore = true;
            return null;
        }
        
        if (replayOptions.getIgnoredCollections().contains(commandResult.collectionName)) {
        	commandResult.ignored++;
        	commandResult.ignore = true;
            return null;
        }
        
        if (replayOptions.getReplayMode() == ReplayMode.READ_ONLY && !commandResult.command.isRead()) {
        	commandResult.ignore = true;
        	return null;
        }
        
        
        return commandResult;
    }
    
    public Document adminCommand(Document command) {
        return mongoClient.getDatabase("admin").runCommand(command);
    }
    
    private BsonDocument getKillCursorsCommandDocument(String collectionName, long cursorId) {
        return new BsonDocument("killCursors", new BsonString(collectionName))
                       .append("cursors", new BsonArray(singletonList(new BsonInt64(cursorId))));
    }

    public ReplayResult replay(BSONObject raw) {
        
    	Document commandDoc = null;
    	CommandResult commandResult = null;
    	String databaseName = null;
        byte[] bytes = (byte[]) raw.get("body");

        if (bytes.length == 0) {
            return null;
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
                    return null;
                }
                if (replayOptions.getIgnoredCollections().contains(collectionName)) {
                    return null;
                }
                
                int nskip = bsonInput.readInt32();
                int nreturn = bsonInput.readInt32();
                
                commandDoc = documentCodec.decode(reader, decoderContext);
                commandResult = processCommand(databaseName, commandDoc);
                //written++;

            } else if (opcode == 2010) {
                int p1 = bsonInput.getPosition();
                databaseName = bsonInput.readCString();
                if (databaseName.equals("local") || databaseName.equals("admin")) {
                    return null;
                }
                String command = bsonInput.readCString();
                commandDoc = documentCodec.decode(reader, decoderContext);
                commandDoc.remove("shardVersion");
                commandResult = processCommand(databaseName, commandDoc);
            } else if (opcode == 2013) {  // OP_MSG
                int flags = bsonInput.readInt32();
                boolean moreSections = true;
                Document k0 = null;
                Document d1 = null;
                int count = 0;
                while (moreSections) {
                    byte kindByte = bsonInput.readByte();
                    count++;
                    if (kindByte == 0) {
                    	
                    	commandDoc = documentCodec.decode(reader, decoderContext);
                        k0 = commandDoc;
                        moreSections = messageLength > bsonInput.getPosition();
                        
                        databaseName = commandDoc.getString("$db");
                        if (databaseName == null || databaseName.equals("local") || databaseName.equals("admin")) {
                            continue;
                        }
                        
                        commandDoc.remove("lsid");
                        commandDoc.remove("$db");
                        commandDoc.remove("$readPreference");
                        
//                        if (! moreSections) {
////                            if (commandDoc.containsKey("count")) {
////                                System.out.println();
////                            }
//                        	commandResult = processCommand(databaseName, commandDoc);
//                        }
                        
                    } else {
                        int p0 = bsonInput.getPosition();
                        int size = bsonInput.readInt32();
                        String seq = bsonInput.readCString();
                        int p1 = bsonInput.getPosition();
                        int remaining = size - (p1 - p0);
                        
                        byte[] mb = new byte[remaining];
                        
                        bsonInput.readBytes(mb);
                        
                        BsonBinaryReader r2 = new BsonBinaryReader(ByteBuffer.wrap(mb));
                        d1 = documentCodec.decode(r2, decoderContext);
                        
                        if (seq == null) {
                        	logger.warn("null seq");
                        	return null;
                        }
                        
//                        if (seq.equals("documents")) {
//                        	commandDoc.put("documents", Arrays.asList(d1));
//                            commandResult = processCommand(databaseName, commandDoc);
//                        } else {
//                        	logger.debug("seq: " + seq);
//                        }
                        
//                        if (commandDoc != null && commandDoc.containsKey("insert")) {
//                            
//                        } else if (commandDoc != null && commandDoc.containsKey("update")) {
//                            commandDoc.put("updates", Arrays.asList(d1));
//                            commandResult = processCommand(databaseName, commandDoc);
//                        } else if (commandDoc != null && commandDoc.containsKey("delete")) {
//                            commandDoc.put("deletes", Arrays.asList(d1));
//                            commandResult = processCommand(databaseName, commandDoc);
//                        } else {
//                            //logger.debug("wtf: " + commandDoc);
//                            return null;
//                        }
                        
                        moreSections = messageLength > bsonInput.getPosition();
                    }
                }
                if (k0 != null && d1 != null) {
                	if (k0.containsKey("insert")) {
                		String collName = k0.getString("insert");
                		commandDoc.put("documents", Arrays.asList(d1));
                		commandResult = processCommand(databaseName, commandDoc);
                	} else if (k0.containsKey("update")) {
                		commandDoc.put("updates", Arrays.asList(d1));
                		commandResult = processCommand(databaseName, commandDoc);
                	} else {
                		//System.out.println("here");
                	}
                } else if (commandDoc.containsKey("find")) {
                	commandResult = processCommand(databaseName, commandDoc);
                } else {
                	//System.out.println("here");
                }
                
                    
            } else {
                logger.warn("ignored opcode: " + opcode);
                //ignored++;
                commandResult.ignore = true;
            }
        }
        
        if (commandResult == null || commandResult.command == null || commandResult.ignore) {
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
            Document executeResult = null;
            if (commandResult.command.isRead()) {
                 
                if (replayOptions.getReadConcern() != null) {
                    commandDoc.put("readConcern", replayOptions.getReadConcern());
                }
                executeResult = mongoClient.getDatabase(db).runCommand(commandDoc, mongoClient.getReadPreference());
                 
            } else {
                
            	BsonDocument wc = replayOptions.getWriteConcern(); 
            	if (wc != null && wc.containsKey("w")) {
            		commandDoc.put("writeConcern", wc);
            	} else {
            		commandDoc.put("writeConcern", DEFAULT_WRITE_CONCERN);
            	}
                
            	executeResult = mongoClient.getDatabase(db).runCommand(commandDoc);
//                if (commandResult.containsKey("writeErrors")) {
//                	//logger.debug("result: " + commandResult);
//                }
                
            }
            long duration = System.nanoTime() - start;
            // long duration = event.stop();
            Number ok = (Number) executeResult.get("ok");
            // logger.debug("result: " + result);
            
            Document cursorDoc = (Document)executeResult.get("cursor");
            if (cursorDoc != null) {
            	long cid =  cursorDoc.getLong("id");
            	if (cid != 0L) {
            		BsonDocument killCursor = getKillCursorsCommandDocument(commandResult.collectionName, cid);
            		try {
            			Document killResult = mongoClient.getDatabase(db).runCommand(killCursor);
            		} catch (MongoCommandException e) {
            		}
            		
            	}
            }
            
            if (ok.equals(1.0)) {
                monitor.incrementEventCount();
                replayResult = new ReplayResult(commandResult.shape, db, commandResult.collectionName, commandResult.command, duration, true);
            } else {
                // event.incrementError(1);
                replayResult = new ReplayResult(commandResult.shape, db, commandResult.collectionName, commandResult.command, duration, false);
                monitor.incrementErrorCount();
            }

        } catch (MongoCommandException e) {
            //e.printStackTrace();
        	if (e.getCode() != 11000) {
        		logger.error(String.format("Error executing command: %s", commandDoc), e);
        	}
            monitor.incrementErrorCount();
        } catch (Exception e) {
            //e.printStackTrace();
            logger.error(String.format("Unexcpected error executing command: %s", commandDoc), e);
            monitor.incrementErrorCount();
        }

        // monitor.add(event);
        return replayResult;
    }
}
