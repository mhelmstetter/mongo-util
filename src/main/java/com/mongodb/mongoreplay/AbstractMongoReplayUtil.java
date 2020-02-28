package com.mongodb.mongoreplay;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.util.CallerBlocksPolicy;
import com.mongodb.util.PausableThreadPoolExecutor;

public abstract class AbstractMongoReplayUtil {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractMongoReplayUtil.class);
    
    private final static String DB_NAME_MAP = "dbNameMap";
    
    private final static long unixToInternal = 62135596800L;
    private final static long internalToUnix = -unixToInternal;

    private final BasicBSONEncoder encoder;

    protected String[] fileNames;
    protected static String[] removeUpdateFields;
    private static Set<String> ignoredCollections = new HashSet<String>();

    private int threads = 8;
    private int queueSize = 250000;
    
    private final static int ONE_MINUTE = 60 * 1000;

    private static Monitor monitor;

    protected PausableThreadPoolExecutor pool = null;
    private BlockingQueue<Runnable> workQueue;
    List<Future<ReplayResult>> futures = new LinkedList<Future<ReplayResult>>();

    private String mongoUriStr;
    private static MongoClient mongoClient;
    ClusterType clusterType;
    
    private int limit = Integer.MAX_VALUE;
    int count = 0;
    int written = 0;
    int ignored = 0;
    int getMoreCount = 0;
    
    private BSONObject firstSeen;
    private BSONObject lastSeen;
    
    private Set<Integer> opcodeWhitelist = new HashSet<Integer>();
    
    private ReplayOptions replayOptions;
    
    //private Set<Long> seenConnections = new HashSet<Long>();
    
    public AbstractMongoReplayUtil() {
        this.encoder = new BasicBSONEncoder();
        opcodeWhitelist.addAll(Arrays.asList(2004, 2010, 2013));
    }

    public void init() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        logger.debug("mongoUriStr: " + mongoUriStr);
        MongoClientURI connectionString = new MongoClientURI(mongoUriStr);
        mongoClient = new MongoClient(connectionString);
        //readPreference = mongoClient.getMongoClientOptions().getReadPreference();
        
        replayOptions.setWriteConcern(mongoClient.getWriteConcern().asDocument());
        
        ReadConcern readConcern = mongoClient.getReadConcern();
        if (readConcern != null && readConcern.getLevel() != null) {
            replayOptions.setReadConcernLevel(readConcern.getLevel());
            
        }
        
        int seedListSize = mongoClient.getAllAddress().size();
        if (seedListSize == 1) {
            logger.warn("Only 1 host specified in seedlist");
        }
        mongoClient.getDatabase("admin").runCommand(new Document("ismaster", 1));
        
        Method method = Mongo.class.getDeclaredMethod("getClusterDescription");
        method.setAccessible(true);
        ClusterDescription cd = (ClusterDescription)method.invoke(mongoClient);
        this.clusterType = cd.getType();
        logger.debug("Connected: " + clusterType);
        
        //workQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        workQueue = new LinkedBlockingQueue<Runnable>(queueSize);
        
        pool = new PausableThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, workQueue, new CallerBlocksPolicy(ONE_MINUTE*5));
        pool.pause();
        
        
        //pool.prestartAllCoreThreads();

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
                
                RawReplayTask rawTask = new RawReplayTask(monitor, mongoClient, replayOptions, raw);
                futures.add(pool.submit(rawTask));

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
        logger.debug(String.format("first event: %s", convertSeen(firstSeen)));
        logger.debug(String.format("last event: %s", convertSeen(lastSeen)));
    }
    
    @SuppressWarnings("static-access")
    protected static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(
                OptionBuilder.withArgName("input mongoreplay bson file(s)").hasArgs().withLongOpt("files").create("f"));

        options.addOption(OptionBuilder.withArgName("remove update fields").hasArgs().withLongOpt("removeUpdateFields")
                .create("u"));

        options.addOption(OptionBuilder.withArgName("limit # operations").hasArg().withLongOpt("limit").create("l"));

        options.addOption(
                OptionBuilder.withArgName("play back target mongo uri").hasArg().withLongOpt("host").isRequired().create("h"));

        options.addOption(OptionBuilder.withArgName("# threads").hasArgs().withLongOpt("threads").create("t"));
        options.addOption(OptionBuilder.withArgName("sleep millis").hasArgs().withLongOpt("sleep").create("s"));
        options.addOption(OptionBuilder.withArgName("queue size").hasArgs().withLongOpt("queue").create("q"));
        
        options.addOption(OptionBuilder.withArgName("ignore collection").hasArgs().withLongOpt("ingoreColl").create("c"));
        
        options.addOption(OptionBuilder.withArgName("db name map").hasArgs().withLongOpt(DB_NAME_MAP).create());
        
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
    
    protected void parseArgs(String args[]) {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        this.replayOptions = new ReplayOptions();
        replayOptions.setIgnoredCollections(ignoredCollections);

        this.fileNames = line.getOptionValues("f");
        String[] x = line.getOptionValues("u");
        replayOptions.setRemoveUpdateFields(x);
        String limitStr = line.getOptionValue("l");

        String mongoUriStr = line.getOptionValue("h");
       
        setMongoUriStr(mongoUriStr);
        
        String sleepStr = line.getOptionValue("s");
        if (sleepStr != null) {
            Long sleep = Long.parseLong(sleepStr);
            replayOptions.setSleepMillis(sleep);
        }

        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            setThreads(threads);
        }

        if (limitStr != null) {
            int limit = Integer.parseInt(limitStr);
            setLimit(limit);
        }
        
        if (line.hasOption("c")) {
            ignoredCollections.addAll(Arrays.asList(line.getOptionValues("c")));
        }
        
        String qStr = line.getOptionValue("q");
        if (qStr != null) {
            int q = Integer.parseInt(qStr);
            setQueueSize(q);
        }
        
        if (line.hasOption(DB_NAME_MAP)) {
        	replayOptions.setDbNameMapString(line.getOptionValue(DB_NAME_MAP));
        }
        
    }

    private void setQueueSize(int q) {
        this.queueSize = q;
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("replayUtil", options);
        System.exit(-1);
    }

    protected void setThreads(int threads) {
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
    
    private static ZonedDateTime convertSeen(BSONObject seen) {
        Long sec = (Long)seen.get("sec");
        Long t = (sec + internalToUnix) * 1000;
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneId.of("UTC"));
    }

}
