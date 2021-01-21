package com.mongodb.mongoreplay;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
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
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonWriter;
import org.bson.BsonWriterSettings;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.UuidCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.mongodb.MongoClient;
import com.mongodb.mongoreplay.opcodes.MessageHeader;

/**
 * Filter a mongoreplay bson file
 *
 */
public class MongoReplayFilter {

    protected static final Logger logger = LoggerFactory.getLogger(MongoReplayFilter.class);

    private final BasicBSONEncoder encoder;
    private final BSONDecoder decoder;
    
    CodecRegistry registry = fromRegistries(fromProviders(new UuidCodecProvider(UuidRepresentation.STANDARD)),
            MongoClient.getDefaultCodecRegistry());
    DocumentCodec documentCodec = new DocumentCodec(registry);

    private String[] removeUpdateFields;

    private int limit = Integer.MAX_VALUE;

    private Map<Integer, Integer> opcodeSeenCounters = new TreeMap<Integer, Integer>();

    private int systemDatabasesSkippedCount = 0;
    int count = 0;
    int written = 0;
    BSONObject obj;
    BSONObject raw;
    BSONObject header;
    MessageHeader parsedHeader;

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

        count = 0;
        written = 0;
        try {
            fos = new FileOutputStream(outputFile);
            FileChannel channel = fos.getChannel();
            while (inputStream.available() > 0) {

                if (count >= limit) {
                    break;
                }
                count++;

                obj = decoder.readObject(inputStream);
                if (obj == null) {
                    break;
                }

                raw = (BSONObject) obj.get("rawop");
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

                header = (BSONObject) raw.get("header");
                int responseto = (Integer) header.get("responseto");
//                if (responseto != 0) {
//                	ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
//                    BsonBinaryReader reader = new BsonBinaryReader(bsonInput);
//
//                    parsedHeader = MessageHeader.parse(bsonInput);
//                    int opcode = (Integer) header.get("opcode");
//                    if (opcode == 2013) {
//                        // Just pass these through
//                        // TODO - we could probably do some filtering, e.g.
//                        // system dbs?
//                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
//                        channel.write(buffer);
//                        written++;
//                    }
//                    
//                    continue;
//                }

                if (header != null) {
                    int opcode = (Integer) header.get("opcode");
                    incrementOpcodeSeenCount(opcode);
                    ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
                    BsonBinaryReader reader = new BsonBinaryReader(bsonInput);

                    parsedHeader = MessageHeader.parse(bsonInput);
                     
                    //logger.debug("opcode: " + opcode + ", headerOpcode: " + headerOpcode);
                    
                    // https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
                    if (opcode == 1) {
                    	int responseFlags = bsonInput.readInt32();
                    	long cursorId = bsonInput.readInt64();
                    	int startingFrom = bsonInput.readInt32();
                    	int nReturned = bsonInput.readInt32();
                    	Document x = documentCodec.decode(reader, DecoderContext.builder().build());
                    
                    } else if (opcode == 2012) {
                        
                        opcode = bsonInput.readInt32();
                        //logger.debug(String.format("Compressed, originalOpcode: %s", opcode));
                        // Dumb hack, just double count the compressed / uncompressed opcode
                        incrementOpcodeSeenCount(opcode);
                        int uncompressedSize = bsonInput.readInt32();
                        byte compressorId = bsonInput.readByte();
                        
                        //logger.debug("compressorId: " + compressorId);
                        
                        int position = bsonInput.getPosition();
                        int remaining = parsedHeader.getMessageLength() - position;
                        byte[] compressed = new byte[remaining];
                        
                        bsonInput.readBytes(compressed);
                        byte[] uncompressed = Snappy.uncompress(compressed);
                        //logger.debug(String.format("compressed.length: %s, uncompressedSize: %s, uncompressed.length: %s", compressed.length, uncompressedSize, uncompressed.length));
                        
                        if (opcode == 2013) {
                            process2013(uncompressed, channel);
                        } else {
                            // TODO I think we can safely ignore these 2004s
                        }
                    } else if (opcode == 2004) {
                        int flags = bsonInput.readInt32();
                        String collectionName = bsonInput.readCString();
                        if (collectionName.equals("admin.$cmd") || collectionName.equals("local.$cmd")) {
                            systemDatabasesSkippedCount++;
                            continue;
                        }
                        int nskip = bsonInput.readInt32();
                        int nreturn = bsonInput.readInt32();
                        Document commandDoc = documentCodec.decode(reader, DecoderContext.builder().build());

                        //System.out.println("2004: " + commandDoc);
                        Document queryCommand = (Document) commandDoc.get("$query");
                        if (queryCommand != null) {
                            commandDoc = queryCommand;
                        }

                        commandDoc.remove("projection");
                        BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
                        BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
                        documentCodec.encode(tmpWriter, commandDoc, EncoderContext.builder().build());
                        int commandDocSize = tmpBuff.getSize();

                        BasicOutputBuffer rawOut = new BasicOutputBuffer();
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);

                        int totalLen = commandDocSize + 28 + collectionName.length();

                        rawOut.writeInt(totalLen);
                        rawOut.writeInt(parsedHeader.getRequestId());
                        rawOut.writeInt(parsedHeader.getResponseTo());
                        rawOut.writeInt(2004);
                        rawOut.writeInt(0);

                        rawOut.writeCString(collectionName);
                        rawOut.writeInt(0); // skip
                        rawOut.writeInt(-1); // return - these values don't seem to matter

                        documentCodec.encode(writer, commandDoc, EncoderContext.builder().build());

                        int size1 = writer.getBsonOutput().getPosition();
                        header.put("messagelength", size1);
                        // System.out.println("obj: " + obj);
                        raw.put("body", rawOut.toByteArray());
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;

                    } else if (opcode == 2010) {
                        header.put("opcode", 2004);
                        int p1 = bsonInput.getPosition();
                        String databaseName = bsonInput.readCString();
                        if (databaseName.equals("local") || databaseName.equals("admin")) {
                        	String command = bsonInput.readCString();
                            systemDatabasesSkippedCount++;
                            continue;
                        }
                        int p2 = bsonInput.getPosition();
                        int databaseNameLen = p2 - p1 + 5; // .$cmd gets
                                                           // appended
                        String command = bsonInput.readCString();
                        Document commandDoc = documentCodec.decode(reader, DecoderContext.builder().build());
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
                        } else if (! command.equals("find")) {
                            //logger.debug(command);
                        }

                        BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
                        BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
                        documentCodec.encode(tmpWriter, commandDoc, EncoderContext.builder().build());
                        int commandDocSize = tmpBuff.getSize();

                        BasicOutputBuffer rawOut = new BasicOutputBuffer();
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);

                        int totalLen = commandDocSize + 28 + databaseNameLen;

                        rawOut.writeInt(totalLen);
                        rawOut.writeInt(parsedHeader.getRequestId());
                        rawOut.writeInt(parsedHeader.getResponseTo());
                        rawOut.writeInt(2010);
                        rawOut.writeInt(0);

                        rawOut.writeCString(databaseName + ".$cmd");
                        rawOut.writeInt(0); // skip
                        rawOut.writeInt(-1); // return - these values don't seem
                                             // to matter

                        documentCodec.encode(writer, commandDoc, EncoderContext.builder().build());

                        int size1 = writer.getBsonOutput().getPosition();

                        header.put("messagelength", size1);

                        raw.put("body", rawOut.toByteArray());
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;

                    } else if (opcode == 2011) {
                        // These are the command replies, we don't need to write
                        // them through
                        Document commandDoc = documentCodec.decode(reader, DecoderContext.builder().build());
                        // System.out.println("doc: " + commandDoc);
                    } else if (opcode == 2013) {
                        // Just pass these through
                        // TODO - we could probably do some filtering, e.g.
                        // system dbs?
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;
                    	
                        //byte[] slice = Arrays.copyOfRange(bytes, 16, bytes.length);
                        //process2013(slice, channel);
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
    
    /**
     * Note this implementation assumes that the header has already been consumed.
     */
    private void process2013(byte[] uncompressed, FileChannel channel) throws IOException {
        ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(uncompressed)));
        BsonBinaryReader reader = new BsonBinaryReader(bsonInput);
        int flagBits = bsonInput.readInt32();
        header.put("opcode", 2013);
        
        BasicOutputBuffer rawOut = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
        
        rawOut.writeInt32(uncompressed.length);
        rawOut.writeInt32(parsedHeader.getRequestId());
        rawOut.writeInt32(parsedHeader.getResponseTo());
        rawOut.writeInt32(parsedHeader.getHeaderOpcode());
        rawOut.writeInt32(flagBits);
        
        int i = 0;
        while (bsonInput.getPosition() < uncompressed.length) {
            //logger.debug(i + " position: " + bsonInput.getPosition() + ", totLen: " + uncompressed.length);
            
            byte kind = bsonInput.readByte();
            rawOut.writeByte(kind);
            
            if (kind == 0) {
                //byte[] slice = Arrays.copyOfRange(uncompressed, 5, uncompressed.length);
                //rawOut.write(slice);
                
                //ByteBufferBsonInput in = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(slice)));
                
                Document mobj = documentCodec.decode(reader, DecoderContext.builder().build());
                String db = (String)mobj.get("$db");
                if (db != null && (db.equals("admin") || db.equals("local"))) {
                    return;
                }
                
                mobj.remove("txnNumber");
                //logger.debug("mobj: " + mobj);
                documentCodec.encode(writer, mobj, EncoderContext.builder().build());
                
            } else if (kind == 1) {
                int sectionSize = bsonInput.readInt32();
                rawOut.writeInt32(sectionSize);
                String sequenceId = bsonInput.readCString();
                rawOut.writeCString(sequenceId);
                int j = 0;
                while (bsonInput.getPosition() < uncompressed.length) {
                    //logger.debug(j + " kind=1, position: " + bsonInput.getPosition() + ", totLen: " + uncompressed.length);
                    
                    
                    // TODO don't understand why we have to do this
                    // if we don't we get:
                    // BsonInvalidOperationException: readStartDocument can only be called when State is VALUE, not when State is DONE.
                    reader = new BsonBinaryReader(bsonInput);
                    
                    Document mobj = documentCodec.decode(reader, DecoderContext.builder().build());
                    documentCodec.encode(writer, mobj, EncoderContext.builder().build());
                    //logger.debug("k1: " + mobj);
                    j++;
                }
            } else {
                logger.error("Unexpected kind byte: " + kind);
            }
            i++;
        }
        int newMessageLength = rawOut.getSize();
        byte[] newMessageLengthBytes = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder()).putInt(newMessageLength).array();
        header.put("messagelength", newMessageLength);
        rawOut.write(newMessageLengthBytes, 0, 4);
        raw.put("body", rawOut.toByteArray());
        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
        channel.write(buffer);
        written++;
    }
    
    
    public BSONObject readObject(final byte[] bytes) {
        BSONCallback bsonCallback = new BasicBSONCallback();
        decode(bytes, bsonCallback);
        return (BSONObject) bsonCallback.get();
    }
    
    private void decode(final byte[] bytes, final BSONCallback callback) {
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
        try {
            BsonWriter writer = new BSONCallbackAdapter(new BsonWriterSettings(), callback);
            writer.pipe(reader);
        } finally {
            reader.close();
        }
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
