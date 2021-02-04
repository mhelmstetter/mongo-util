package com.mongodb.mongoreplay;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitModeReplayTask implements Callable<List<ReplayResult>> {

    // private TimedEvent event;
    
    private Document commandDoc;
    private String databaseName;
    private String collectionName;
    private Command command;
    private String queryShape;
    
    private ReplayOptions replayOptions;
    
    private InputStream inputStream;
    private BSONObject raw;
    private boolean ignore = false;
    
    private int limit = Integer.MAX_VALUE;
    int count = 0;
    int written = 0;
    int ignored = 0;
    int getMoreCount = 0;
    
    private BSONObject firstSeen;
    private BSONObject lastSeen;
    
    private Set<Integer> opcodeWhitelist = new HashSet<Integer>();
    
    private final static DocumentCodec documentCodec = new DocumentCodec();
    private final static DecoderContext decoderContext = DecoderContext.builder().build();

    protected static final Logger logger = LoggerFactory.getLogger(SplitModeReplayTask.class);
    
    Replayer replayer;
    List<ReplayResult> replayResults = new LinkedList<>();

    public SplitModeReplayTask(Replayer replayer, InputStream inputStream) {
        this.replayer = replayer;
        this.inputStream = inputStream;
        opcodeWhitelist.addAll(Arrays.asList(2004, 2010, 2013));
    }
    
    private void process() {
    	BSONDecoder decoder = new BasicBSONDecoder();
        
        try {
            while (inputStream.available() > 0) {

                if (count >= limit) {
                    break;
                }

                BSONObject obj = decoder.readObject(inputStream);
                if (obj == null) {
                    break;
                }

                BSONObject raw = (BSONObject) obj.get("rawop");
                
                if (raw == null) {
                    continue;
                }
                BSONObject header = (BSONObject) raw.get("header");
                int opcode = (Integer) header.get("opcode");
                if (! opcodeWhitelist.contains(opcode)) {
                	ignored++;
                    continue;
                }
                
                Long seenconnectionnum = (Long)obj.get("seenconnectionnum");
                //seenConnections.add(seenconnectionnum);
                
                lastSeen = (BSONObject) obj.get("seen");
                if (count == 0) {
                    firstSeen = lastSeen;
                }
                
                ReplayResult result = replayer.replay(raw);
                if (result != null) {
                	replayResults.add(result);
                }
                

                count++;
//                if ((count % 100000) == 0) {
//                    logger.debug("workQueue size " + workQueue.size());
//                    //logger.debug("seenConnections: " + seenConnections.size());
//                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
            }
        }
        
        logger.debug(String.format("%s objects read, %s filtered objects written, %s ignored", count, written, ignored));
        logger.debug(String.format("%s getMore", getMoreCount));
        //logger.debug(String.format("first event: %s", convertSeen(firstSeen)));
        //logger.debug(String.format("last event: %s", convertSeen(lastSeen)));
        
    	
    }
    
    

    @Override
    public List<ReplayResult> call() {
        process();
        return replayResults;
    }
}
