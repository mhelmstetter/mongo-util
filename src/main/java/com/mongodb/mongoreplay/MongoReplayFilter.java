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
import java.util.Map;
import java.util.TreeMap;
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
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Filter a mongoreplay bson file
 *
 */
public class MongoReplayFilter {

    protected static final Logger logger = LoggerFactory.getLogger(MongoReplayFilter.class);

    private final BasicBSONEncoder encoder;
    private final BSONDecoder decoder;

    private String[] removeUpdateFields;

    private int limit = Integer.MAX_VALUE;

    private Map<Integer, Integer> opcodeSeenCounters = new TreeMap<Integer, Integer>();

    private int systemDatabasesSkippedCount = 0;

    public MongoReplayFilter() {
        this.encoder = new BasicBSONEncoder();
        this.decoder = new BasicBSONDecoder();
    }

    @SuppressWarnings({ "unused", "unchecked" })
    public void filterFile(String filename) throws FileNotFoundException, DataFormatException {
        logger.debug("filterFile: " + filename);
        File file = new File(filename);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        File outputFile = new File(filename + ".FILTERED");
        FileOutputStream fos = null;

        int count = 0;
        int written = 0;
        try {
            fos = new FileOutputStream(outputFile);
            FileChannel channel = fos.getChannel();
            while (inputStream.available() > 0) {

                if (count >= limit) {
                    break;
                }
                count++;

                BSONObject obj = decoder.readObject(inputStream);
                if (obj == null) {
                    break;
                }

                BSONObject raw = (BSONObject) obj.get("rawop");
                if (raw == null) {
                    logger.trace("raw was null");
                    ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                    channel.write(buffer);
                    continue;
                }
                byte[] bytes = (byte[]) raw.get("body");

                if (bytes.length == 0) {
                    logger.trace("body length was 0");
                    continue;
                }

                BSONObject header = (BSONObject) raw.get("header");

                if (header != null) {
                    int opcode = (Integer) header.get("opcode");
                    incrementOpcodeSeenCount(opcode);
                    ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
                    BsonBinaryReader reader = new BsonBinaryReader(bsonInput);

                    int messageLength = bsonInput.readInt32();
                    int requestId = bsonInput.readInt32();
                    int responseTo = bsonInput.readInt32();
                    int headerOpcode = bsonInput.readInt32();
                    
                    logger.debug("opcode: " + opcode + ", headerOpcode: " + headerOpcode);
                    
                    // https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
                    if (opcode == 2012) {
                        
                        opcode = bsonInput.readInt32();
                        // Dumb hack, just double count the compressed / uncompressed opcode
                        incrementOpcodeSeenCount(opcode);
                        int uncompressedSize = bsonInput.readInt32();
                        byte compressorId = bsonInput.readByte();
                        
                        logger.debug("compressorId: " + compressorId);
                        
                        
                        int position = bsonInput.getPosition();
                        int remaining = messageLength - position;
                        
                        byte[] compressed = new byte[remaining];
                        
                        bsonInput.readBytes(compressed);
                        byte[] uncompressed = Snappy.uncompress(compressed);
                        
                        //bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
                        obj = decoder.readObject(uncompressed);
                    }

                    if (opcode == 2004) {
                        int flags = bsonInput.readInt32();
                        String collectionName = bsonInput.readCString();
                        if (collectionName.equals("admin.$cmd") || collectionName.equals("local.$cmd")) {
                            systemDatabasesSkippedCount++;
                            continue;
                        }
                        int nskip = bsonInput.readInt32();
                        int nreturn = bsonInput.readInt32();
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());

                        Document queryCommand = (Document) commandDoc.get("$query");
                        if (queryCommand != null) {
                            commandDoc = queryCommand;
                        }

                        commandDoc.remove("projection");
                        BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
                        BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
                        new DocumentCodec().encode(tmpWriter, commandDoc, EncoderContext.builder().build());
                        int commandDocSize = tmpBuff.getSize();

                        BasicOutputBuffer rawOut = new BasicOutputBuffer();
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);

                        int totalLen = commandDocSize + 28 + collectionName.length();

                        rawOut.writeInt(totalLen);
                        rawOut.writeInt(requestId);
                        rawOut.writeInt(responseTo);
                        rawOut.writeInt(2004);
                        rawOut.writeInt(0);

                        rawOut.writeCString(collectionName);
                        rawOut.writeInt(0); // skip
                        rawOut.writeInt(-1); // return - these values don't seem
                                             // to matter

                        new DocumentCodec().encode(writer, commandDoc, EncoderContext.builder().build());

                        int size1 = writer.getBsonOutput().getPosition();

                        header.put("messagelength", size1);

                        // System.out.println("obj: " + obj);
                        raw.put("body", rawOut.toByteArray());
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;

                        // ByteBuffer buffer =
                        // ByteBuffer.wrap(encoder.encode(obj));
                        // channel.write(buffer);
                        // written++;

                    } else if (opcode == 2010) {
                        header.put("opcode", 2004);
                        int p1 = bsonInput.getPosition();
                        String databaseName = bsonInput.readCString();
                        if (databaseName.equals("local") || databaseName.equals("admin")) {
                            systemDatabasesSkippedCount++;
                            continue;
                        }
                        int p2 = bsonInput.getPosition();
                        int databaseNameLen = p2 - p1 + 5; // .$cmd gets
                                                           // appended
                        String command = bsonInput.readCString();
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        Document queryCommand = (Document) commandDoc.get("$query");
                        commandDoc.remove("shardVersion");
                        commandDoc.remove("projection");

                        if (command.equals("update")) {
                            List<Document> updates = (List<Document>) commandDoc.get("updates");
                            for (Document updateDoc : updates) {
                                Document query = (Document) updateDoc.get("q");

                                if (removeUpdateFields != null) {
                                    for (String fieldName : removeUpdateFields) {
                                        query.remove(fieldName);
                                    }
                                }

                            }
                        }

                        BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
                        BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
                        new DocumentCodec().encode(tmpWriter, commandDoc, EncoderContext.builder().build());
                        int commandDocSize = tmpBuff.getSize();

                        BasicOutputBuffer rawOut = new BasicOutputBuffer();
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);

                        int totalLen = commandDocSize + 28 + databaseNameLen;

                        rawOut.writeInt(totalLen);
                        rawOut.writeInt(requestId);
                        rawOut.writeInt(responseTo);
                        rawOut.writeInt(2010);
                        rawOut.writeInt(0);

                        rawOut.writeCString(databaseName + ".$cmd");
                        rawOut.writeInt(0); // skip
                        rawOut.writeInt(-1); // return - these values don't seem
                                             // to matter

                        new DocumentCodec().encode(writer, commandDoc, EncoderContext.builder().build());

                        int size1 = writer.getBsonOutput().getPosition();

                        header.put("messagelength", size1);

                        // System.out.println("obj: " + obj);
                        raw.put("body", rawOut.toByteArray());
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;

                        // System.out.println(" c: '" + databaseName + "' " +
                        // command + " doc: " + commandDoc);
                    } else if (opcode == 2011) {
                        // These are the command replies, we don't need to write
                        // them through
                        Document commandDoc = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                        // System.out.println("doc: " + commandDoc);
                    } else if (opcode == 2013) {
                        // Just pass these through
                        // TODO - we could probably do some filtering, e.g.
                        // system dbs?
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;
                    }
                } else {
                    logger.debug("Header was null, WTF?");
                }

                // count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
            }
        }
        logCounts();
        System.err.println(String.format("%s objects read, %s filtered objects written", count, written));
    }
    

    private void logCounts() {
        for (Map.Entry<Integer, Integer> entry : opcodeSeenCounters.entrySet()) {
            logger.debug(String.format("opcode: %4s count: %,10d", entry.getKey(), entry.getValue()));
        }
        logger.debug(String.format("systemDatabasesSkippedCount: : %,10d", systemDatabasesSkippedCount));
    }

    private void incrementOpcodeSeenCount(int opcode) {
        Integer count = opcodeSeenCounters.getOrDefault(opcode, 0);
        opcodeSeenCounters.put(opcode, ++count);
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
