package com.mongodb.mongoreplay;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.ByteBufferBsonInput;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.mongoreplay.undo.UndoThread;
import com.mongodb.mongoreplay.undo.UndoUpdate;

/**
 * Filter a mongoreplay bson file
 *
 */
public class MongoReplayFilter {

    private final BasicBSONEncoder encoder;
    
    private String[] removeUpdateFields;
    
    //BlockingQueue<Object> workQueue = new ArrayBlockingQueue<>(10000);
    //private MongoClient mongoClient;
    
    private boolean reverseOps = true;
    
    private int limit = Integer.MAX_VALUE;

    public MongoReplayFilter() {
        this.encoder = new BasicBSONEncoder();
        
//        MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017");
//        mongoClient = new MongoClient(connectionString);
//        mongoClient.getDatabase("admin").runCommand(new Document("ping",1));
//        
//        UndoThread workerThread = new UndoThread(workQueue, mongoClient);
//        workerThread.start();
    }
    

    public void filterFile(String filename) throws FileNotFoundException, DataFormatException {

        File file = new File(filename);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        File outputFile = new File(filename + ".FILTERED");
        FileOutputStream fos = null;

        BSONDecoder decoder = new BasicBSONDecoder();
        int count = 0;
        int written = 0;
        try {
            fos = new FileOutputStream(outputFile);
            FileChannel channel = fos.getChannel();
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
                   ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                   channel.write(buffer);
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
                        //System.out.println("skip: " + nskip + " return: " + nreturn);
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        
                        Document queryCommand = (Document)commandDoc.get("$query");
                        if (queryCommand != null) {
                            commandDoc = queryCommand;
                        } else {
                            //System.out.println("**" + commandDoc);
                        }
                        
                        commandDoc.remove("projection");
                        //System.out.println("2004 c: '" + collectionName + "' flags: " + commandDoc);
                        
                        ByteBufferBsonOutput tmpBuff = new ByteBufferBsonOutput(new SimpleBufferProvider());
                        BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
                        new DocumentCodec().encode(tmpWriter, commandDoc, EncoderContext.builder().build());
                        int commandDocSize = tmpBuff.getSize();
                        
                        ByteBufferBsonOutput rawOut = new ByteBufferBsonOutput(new SimpleBufferProvider());
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
                        
                        int totalLen = commandDocSize + 28 + collectionName.length();
                        
                        rawOut.writeInt(totalLen);
                        rawOut.writeInt(requestId);
                        rawOut.writeInt(responseTo);
                        rawOut.writeInt(2004);
                        rawOut.writeInt(0);
                        
                        rawOut.writeCString(collectionName);
                        rawOut.writeInt(0); // skip
                        rawOut.writeInt(-1); // return - these values don't seem to matter
                        
                        new DocumentCodec().encode(writer, commandDoc, EncoderContext.builder().build());
                        
                        int size1 = writer.getBsonOutput().getPosition();
                        
                        header.put("messagelength", size1);
                        
                        //System.out.println("obj: " + obj);
                        raw.put("body", rawOut.toByteArray());
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;
                        
//                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
//                        channel.write(buffer);
//                        written++;
                        
                    } else if (opcode == 2010) {
                        
                        header.put("opcode", 2004);
                        
                        int p1 = bsonInput.getPosition();
                        
                        String databaseName = bsonInput.readCString();
                        if (databaseName.equals("local") || databaseName.equals("admin")) {
                            continue;
                        }
                        int p2 = bsonInput.getPosition();
                        int databaseNameLen = p2 - p1 + 5; // .$cmd gets appended
                        String command = bsonInput.readCString();
                        //p1 = bsonInput.getPosition();
                        //int commandLen = p1 - p2;
                        
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        
                        Document queryCommand = (Document)commandDoc.get("$query");
                        if (queryCommand != null) {
                            System.out.println("$query: " + queryCommand);
                        } else {
                            System.out.println("**" + commandDoc);
                        }
                        
                        commandDoc.remove("shardVersion");
                        commandDoc.remove("projection");
                        //System.out.println(commandDoc);
                        
                        if (command.equals("update")) {
                            List<Document> updates = (List<Document>)commandDoc.get("updates");
                            for (Document updateDoc : updates) {
                                Document query = (Document)updateDoc.get("q");
                                System.out.println(commandDoc.get("update") + " " + query);
                                
                                if (removeUpdateFields != null) {
                                    for (String fieldName : removeUpdateFields) {
                                        query.remove(fieldName);
                                    }
                                }
                                
//                                if (reverseOps) {
//                                    workQueue.add(new UndoUpdate(query));
//                                }
                                
                                
                                //Document u = (Document)updateDoc.get("u");
                            }
                        } else {
                            //System.out.println(command);
                        }
                        
                        ByteBufferBsonOutput tmpBuff = new ByteBufferBsonOutput(new SimpleBufferProvider());
                        BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
                        new DocumentCodec().encode(tmpWriter, commandDoc, EncoderContext.builder().build());
                        int commandDocSize = tmpBuff.getSize();
                        
                        ByteBufferBsonOutput rawOut = new ByteBufferBsonOutput(new SimpleBufferProvider());
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
                        
                        int totalLen = commandDocSize + 28 + databaseNameLen;
                        
                        rawOut.writeInt(totalLen);
                        rawOut.writeInt(requestId);
                        rawOut.writeInt(responseTo);
                        rawOut.writeInt(2010);
                        rawOut.writeInt(0);
                        
                        rawOut.writeCString(databaseName + ".$cmd");
                        rawOut.writeInt(0); // skip
                        rawOut.writeInt(-1); // return - these values don't seem to matter
                        
                        new DocumentCodec().encode(writer, commandDoc, EncoderContext.builder().build());
                        
                        int size1 = writer.getBsonOutput().getPosition();
                        
                        header.put("messagelength", size1);
                        
                        //System.out.println("obj: " + obj);
                        raw.put("body", rawOut.toByteArray());
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;
                        
                        //System.out.println(" c: '" + databaseName + "' " + command + " doc: " + commandDoc);
                    } else if (opcode == 2011) {
                        
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        System.out.println("doc: " + commandDoc);
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
        System.err.println(String.format("%s objects read, %s filtered objects written", count, written));
    }
    
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(
                OptionBuilder.withArgName("input mongoreplay bson file(s)").hasArgs().withLongOpt("files").create("f"));
        
        options.addOption(
                OptionBuilder.withArgName("remove update fields").hasArgs().withLongOpt("removeUpdateFields").create("u"));
        
        options.addOption(
                OptionBuilder.withArgName("limit # operations").hasArg().withLongOpt("limit").create("l"));
    
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
        
        MongoReplayFilter filter = new MongoReplayFilter();
        filter.setRemoveUpdateFields(removeUpdateFields);
        
        if (limitStr != null) {
            int limit = Integer.parseInt(limitStr);
            filter.setLimit(limit);
        }
        
        for (String filename : fileNames) {
            filter.filterFile(filename);
        }
        

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

}
