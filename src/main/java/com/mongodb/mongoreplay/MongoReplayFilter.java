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
import java.util.ArrayList;
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
import org.bson.BSONException;
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
import com.mongodb.mongoreplay.Replayer.CommandResult;
import com.mongodb.mongoreplay.opcodes.MessageHeader;

/**
 * Filter a mongoreplay bson file
 *
 */
public class MongoReplayFilter {

    protected static final Logger logger = LoggerFactory.getLogger(MongoReplayFilter.class);

    private final BasicBSONEncoder encoder;
    private final BSONDecoder decoder;
    
    private final static DecoderContext decoderContext = DecoderContext.builder().build();
    
    CodecRegistry registry = fromRegistries(fromProviders(new UuidCodecProvider(UuidRepresentation.STANDARD)),
            MongoClient.getDefaultCodecRegistry());
    DocumentCodec documentCodec = new DocumentCodec(registry);

    private String[] removeUpdateFields;

    private int limit = Integer.MAX_VALUE;
    
    private int splits = 1;

    private Map<Integer, Integer> opcodeSeenCounters = new TreeMap<Integer, Integer>();

    private int systemDatabasesSkippedCount = 0;
    int count = 0;
    int written = 0;
    BSONObject obj;
    BSONObject raw;
    BSONObject header;
    MessageHeader parsedHeader;
    
    private List<FileChannel> fileChannels;

    public MongoReplayFilter() {
        this.encoder = new BasicBSONEncoder();
        this.decoder = new BasicBSONDecoder();
    }
    
    private void createOutputFiles(String filename) throws FileNotFoundException {
    	//outputFiles = new ArrayList<>(splits);
    	fileChannels = new ArrayList<>(splits);
    	for (int i = 1; i <= splits; i++) {
    		File outputFile = new File(String.format("%s.%s.FILTERED", filename, i));
    		FileOutputStream fos = new FileOutputStream(outputFile);
    		fileChannels.add(fos.getChannel());
    	}
    }
    
    private FileChannel getOutputFileChannel(Long seenNum) {
    	int index = 0;
    	if (seenNum != null) {
    		index = seenNum.intValue() % splits;
    	}
    	return fileChannels.get(index);
    }

    @SuppressWarnings({ "unused", "unchecked" })
    public void filterFile(String filename) throws FileNotFoundException, DataFormatException {
        logger.debug("filterFile: " + filename);
        File file = new File(filename);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        
        createOutputFiles(filename);

        count = 0;
        written = 0;
        try {
            
            while (inputStream.available() > 0) {

                if (count >= limit) {
                    break;
                }
                count++;

                obj = decoder.readObject(inputStream);
                if (obj == null) {
                    break;
                }
                Long seenconnectionnum = (Long)obj.get("seenconnectionnum");
                //logger.debug("seen: " + seenconnectionnum);
                
                FileChannel channel = getOutputFileChannel(seenconnectionnum);

                raw = (BSONObject) obj.get("rawop");
                if (raw == null) {
                    logger.trace("raw was null");
                    ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                    channel.write(buffer);
                    continue;
                }
                byte[] bodyBytes = (byte[]) raw.get("body");

                if (bodyBytes.length == 0) {
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
                    ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bodyBytes)));
                    BsonBinaryReader reader = new BsonBinaryReader(bsonInput);

                    parsedHeader = MessageHeader.parse(bsonInput);
                     
                    //logger.debug("opcode: " + opcode + ", headerOpcode: " + headerOpcode);
                    
                    // https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
                    if (opcode == 1) {
                    	int responseFlags = bsonInput.readInt32();
                    	long cursorId = bsonInput.readInt64();
                    	int startingFrom = bsonInput.readInt32();
                    	int nReturned = bsonInput.readInt32();
                    	try {
                    		Document x = documentCodec.decode(reader, DecoderContext.builder().build());
                    	} catch (BSONException be) {
                    	}
                    	
                    
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
                            //p2013(uncompressed, channel);
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
                    	
                    	int messageLength = parsedHeader.getMessageLength();
                    	header.put("messagelength", messageLength-4);
                    	
                    	int pos = bsonInput.getPosition();

                    	BasicOutputBuffer rawOut = new BasicOutputBuffer();
                        BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
                        rawOut.writeInt(messageLength-4);
                        rawOut.writeInt(parsedHeader.getRequestId());
                        rawOut.writeInt(parsedHeader.getResponseTo());
                        rawOut.writeInt(2013);
                        rawOut.writeInt(0); // flags
                        byte[] slice = Arrays.copyOfRange(bodyBytes, 20, messageLength-4);
                        rawOut.write(slice);
                        
                    	raw.put("body", rawOut.toByteArray());
                    	
                    	ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                    	
//                    	
//                    	
//                    	boolean hasCRC = (flags & 1) == 1;
//                    	
//                    	if (hasCRC) {
//                    		
//                    		
//
//                    		BasicOutputBuffer headerOut = new BasicOutputBuffer();
//                            BsonBinaryWriter headerWriter = new BsonBinaryWriter(headerOut);
//                            
////                            headerWriter.writeInt32(messageLength);
////                            headerWriter.writeInt32(parsedHeader.getRequestId());
////                            headerWriter.writeInt32(parsedHeader.getResponseTo());
////                            headerWriter.writeInt32(2013);
//
////                           
//                            
//                            ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(header));
//                            //channel.write(headerWriter.);
//                            
//                            //documentCodec.encode(headerWriter, header, EncoderContext.builder().build());
//                            
//                            raw.put("header", headerOut.toByteArray());
//
////                            BasicOutputBuffer rawOut = new BasicOutputBuffer();
////                            BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
////                            
////                            rawOut.write(bytes);
//                            
//                            //ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
//                            
//                            raw.put("body", bytes);
//                            buffer = ByteBuffer.wrap(encoder.encode(obj));
//                            channel.write(buffer);
//                            
//
////                            documentCodec.encode(writer, commandDoc, EncoderContext.builder().build());
////
////                            int size1 = writer.getBsonOutput().getPosition();
////                            header.put("messagelength", size1);
////                            // System.out.println("obj: " + obj);
////                            raw.put("body", rawOut.toByteArray());
////                            ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
////                            channel.write(buffer);
////                            written++;
////                            
////                    		System.out.println("removing crc");
////                    		
////                    		BasicOutputBuffer rawOut = new BasicOutputBuffer();
////                            BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
////                            
////                            
////
////                            
////                            rawOut.writeInt(parsedHeader.getRequestId());
////                            rawOut.writeInt(parsedHeader.getResponseTo());
////                            rawOut.writeInt(2013);
////                            channel.write(ByteBuffer.wrap(rawOut.getInternalBuffer()));
//                    		
//                    		
////                          
//                    	} else {
//                    		ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
//                            channel.write(buffer);
//                            
//                    		
//                    	}
//                    	
//                        
////                        written++;
////                        
////                        logger.debug("header len: " + parsedHeader.getMessageLength());
////                    	
////                        // bytes.length
////                        
////                        //byte[] slice = Arrays.copyOfRange(bytes, 16, parsedHeader.getMessageLength());
////                        p2013(bytes, channel);
                    	
                    }
                } else {
                    logger.debug("Header was null, WTF?");
                }

                // count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
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
    
    private void p2013(byte[] uncompressed, FileChannel channel) throws IOException {
    	
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
        
        Document commandDoc = null;
    	CommandResult commandResult = null;
    	String databaseName = null;
    	int messageLength = uncompressed.length;
    	
    	logger.debug("p2013 len: " + messageLength);
        
        int i = 0;
        while (bsonInput.getPosition() < uncompressed.length) {
        	int flags = bsonInput.readInt32();
            boolean moreSections = true;
            while (moreSections) {
                byte kindByte = bsonInput.readByte();
                
                if (kindByte == 0) {
                	
                	Document mobj = documentCodec.decode(reader, DecoderContext.builder().build());
                	
                    //commandDoc = documentCodec.decode(reader, decoderContext);
                    
                    moreSections = messageLength > bsonInput.getPosition();
                    
                    databaseName = commandDoc.getString("$db");
                    if (databaseName == null || databaseName.equals("local") || databaseName.equals("admin")) {
                        continue;
                    }
                    
                    commandDoc.remove("lsid");
                    commandDoc.remove("$db");
                    commandDoc.remove("$readPreference");
                    
                    if (! moreSections) {
//                        if (commandDoc.containsKey("count")) {
//                            System.out.println();
//                        }
                    	//commandResult = processCommand(databaseName, commandDoc);
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
                        //commandResult = processCommand(databaseName, commandDoc);
                    } else if (commandDoc != null && commandDoc.containsKey("update")) {
                        commandDoc.put("updates", Arrays.asList(d1));
                        //commandResult = processCommand(databaseName, commandDoc);
                    } else if (commandDoc != null && commandDoc.containsKey("delete")) {
                        commandDoc.put("deletes", Arrays.asList(d1));
                        //commandResult = processCommand(databaseName, commandDoc);
                    } else {
                        logger.debug("wtf: " + commandDoc);
                    }
                    
                    moreSections = messageLength > bsonInput.getPosition();
                }
            }
        }
    	
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
            
            
            byte kind = bsonInput.readByte();
            
            logger.debug(i + " position: " + bsonInput.getPosition() + ", totLen: " + uncompressed.length + " kind: " + kind);
            
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
                logger.debug("mobj: " + mobj);
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
        
        options.addOption(OptionBuilder.withArgName("number of output files split").hasArgs().withLongOpt("split").create("s"));

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
        
        String splitStr = line.getOptionValue("s");
        if (splitStr != null) {
            int splits = Integer.parseInt(splitStr);
            filter.setSplits(splits);
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

	public void setSplits(int splits) {
		this.splits = splits;
	}

}
