package com.mongodb.mongoreplay;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BsonBinaryReader;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.io.ByteBufferBsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.util.Monitor;

public class MongoReplay {

    protected static final Logger logger = LoggerFactory.getLogger(MongoReplay.class);

    private final BasicBSONEncoder encoder;

    private String[] removeUpdateFields;

    private int threads = 8;
    private int queueSize = 1000000;

    private Monitor monitor;

    private ThreadPoolExecutor pool = null;
    private BlockingQueue<Runnable> workQueue;

    private String mongoUriStr;
    private MongoClient mongoClient;
    ClusterType clusterType;
    
    private ReadPreference readPref;


    private int limit = Integer.MAX_VALUE;
    int count = 0;
    int written = 0;
    int ignored = 0;
    int getMoreCount = 0;

    public MongoReplay() {
        this.encoder = new BasicBSONEncoder();
    }

    public void init() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        MongoClientURI connectionString = new MongoClientURI(mongoUriStr);
        mongoClient = new MongoClient(connectionString);
        readPref = mongoClient.getMongoClientOptions().getReadPreference();
        Document result = mongoClient.getDatabase("admin").runCommand(new Document("ismaster", 1));
        
        
        Method method = Mongo.class.getDeclaredMethod("getClusterDescription");
        method.setAccessible(true);
        ClusterDescription cd = (ClusterDescription)method.invoke(mongoClient);
        this.clusterType = cd.getType();
        logger.debug("Connected: " + readPref + " " + clusterType);
        
        workQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        pool = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, workQueue);
        pool.prestartAllCoreThreads();

        monitor = new Monitor(Thread.currentThread());
        monitor.setPool(pool);
        monitor.start();
    }

    public void close() {
        pool.shutdown();

        while (!pool.isTerminated()) {
            Thread.yield();
            try {
                Thread.sleep(5000);
                logger.debug("Waiting for pool");
            } catch (InterruptedException e) {
                // reset interrupted status
                Thread.interrupted();
            }
        }

        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // reset interrupted status
            Thread.interrupted();
            if (null != monitor && monitor.isAlive()) {
                logger.error("interrupted error", e);
            }
            // harmless - this means the monitor wants to exit
            // if anything went wrong, the monitor will log it
            logger.warn("interrupted while waiting for pool termination");
        }

        halt();
        mongoClient.close();
        logger.debug("close() complete");
    }

    private void halt() {
        if (null != pool) {
            pool.shutdownNow();
        }

        while (null != monitor && monitor.isAlive()) {
            try {
                monitor.halt();
                // wait for monitor to exit
                monitor.join();
            } catch (InterruptedException e) {
                // reset interrupted status and ignore
                Thread.interrupted();
            }
        }

        if (Thread.currentThread().isInterrupted()) {
            logger.debug("resetting thread status");
            Thread.interrupted();
        }
    }

    public void replayFile(String filename) throws FileNotFoundException, DataFormatException {

        File file = new File(filename);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        BSONDecoder decoder = new BasicBSONDecoder();
        
        try {

            int i = 0;
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
                byte[] bytes = (byte[]) raw.get("body");

                if (bytes.length == 0) {
                    continue;
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
                        if (collectionName.equals("admin.$cmd") || collectionName.equals("local.$cmd")) {
                            continue;
                        }
                        int nskip = bsonInput.readInt32();
                        int nreturn = bsonInput.readInt32();
                        
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        
                        String databaseName = StringUtils.substringBefore(collectionName, ".$cmd");
                        processCommand(commandDoc, databaseName);

                        written++;

                    } else if (opcode == 2010) {
                        int p1 = bsonInput.getPosition();
                        
                        String databaseName = bsonInput.readCString();
                        if (databaseName.equals("local") || databaseName.equals("admin")) {
                            continue;
                        }
                        int p2 = bsonInput.getPosition();
                        
                        String command = bsonInput.readCString();
                        //p1 = bsonInput.getPosition();
                        //int commandLen = p1 - p2;
                        
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        commandDoc.remove("shardVersion");
                        processCommand(commandDoc, databaseName);
                        System.out.println(commandDoc);
                    } else if (opcode == 2011) {
                        ignored++;
                    }
                }

                count++;
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
    }
    
    private void processCommand(Document commandDoc, String databaseName) {
        //System.out.println(commandDoc);
        CommandType commandType = CommandType.READ;
        if (commandDoc.containsKey("$query")) {
            Document queryDoc = (Document)commandDoc.get("$query");
            commandDoc = queryDoc;
            
//            if (clusterType.equals(ClusterType.REPLICA_SET)) {
//                commandDoc = queryDoc;
//            } else {
//                if (readPref.isSlaveOk()) {
//                    Document rp = (Document)commandDoc.get("$readPreference");
//                    if (rp != null) {
//                        rp.put("mode", readPref.getName());
//                    }
//                }
//                
//            }
            
            
            
        } else if (commandDoc.containsKey("query")) {
            
        } else if (commandDoc.containsKey("find")) {
//            if (readPref.isSlaveOk() && clusterType.equals(ClusterType.SHARDED)) {
//                commandDoc = new Document("$query", commandDoc);
//                commandDoc.put("$readPreference", new Document("mode", readPref.getName()));
//            }
            
            
        }  else if (commandDoc.containsKey("insert")) {
            commandType = CommandType.WRITE;
        }  else if (commandDoc.containsKey("update")) {
            commandType = CommandType.WRITE;
        }  else if (commandDoc.containsKey("getMore")) {
            getMoreCount++;
            ignored++;
            return;
        } else {
            logger.warn("ignored command: " + commandDoc);
            ignored++;
        }

        pool.submit(new ReplayTask(monitor, mongoClient, commandDoc, commandType, databaseName));
    }

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(
                OptionBuilder.withArgName("input mongoreplay bson file(s)").hasArgs().withLongOpt("files").create("f"));

        options.addOption(OptionBuilder.withArgName("remove update fields").hasArgs().withLongOpt("removeUpdateFields")
                .create("u"));

        options.addOption(OptionBuilder.withArgName("limit # operations").hasArg().withLongOpt("limit").create("l"));

        options.addOption(
                OptionBuilder.withArgName("play back target mongo uri").hasArgs().withLongOpt("host").create("h"));

        options.addOption(OptionBuilder.withArgName("# threads").hasArgs().withLongOpt("threads").create("t"));
        
        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit(options);
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit(options);
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(options);
        }

        String[] fileNames = line.getOptionValues("f");

        if (fileNames == null) {
            printHelpAndExit(options);
        }

        return line;
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    public static void main(String args[]) throws Exception {

        CommandLine line = initializeAndParseCommandLineOptions(args);

        String[] fileNames = line.getOptionValues("f");
        String[] removeUpdateFields = line.getOptionValues("u");
        String limitStr = line.getOptionValue("l");

        String mongoUriStr = line.getOptionValue("h");

        MongoReplay filter = new MongoReplay();
        filter.setRemoveUpdateFields(removeUpdateFields);
        filter.setMongoUriStr(mongoUriStr);

        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            filter.setThreads(threads);
        }

        if (limitStr != null) {
            int limit = Integer.parseInt(limitStr);
            filter.setLimit(limit);
        }

        filter.init();

        for (String filename : fileNames) {
            filter.replayFile(filename);
        }

        filter.close();

    }

    private void setThreads(int threads) {
        this.threads = threads;
    }

    public String[] getRemoveUpdateFields() {
        return removeUpdateFields;
    }

    public void setRemoveUpdateFields(String[] removeUpdateFields) {
        this.removeUpdateFields = removeUpdateFields;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setMongoUriStr(String mongoUriStr) {
        this.mongoUriStr = mongoUriStr;
    }

}
