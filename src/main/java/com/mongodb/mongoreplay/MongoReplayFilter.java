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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
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
import org.bson.BsonSerializationException;
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
import com.mongodb.mongoreplay.opcodes.Section;

/**
 * Filter a mongoreplay bson file
 *
 */
public class MongoReplayFilter {

	protected static final Logger logger = LoggerFactory.getLogger(MongoReplayFilter.class);

	private final BasicBSONEncoder encoder;
	private final BSONDecoder decoder;
	private static final EncoderContext encoderContext = EncoderContext.builder().build();

	//private final static DecoderContext decoderContext = DecoderContext.builder().build();

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
	private FileChannel channel;
	
	LinkedList<Document> documents = new LinkedList<>();

	public MongoReplayFilter() {
		this.encoder = new BasicBSONEncoder();
		this.decoder = new BasicBSONDecoder();
	}

	private void createOutputFiles(String filename) throws FileNotFoundException {
		// outputFiles = new ArrayList<>(splits);
		fileChannels = new ArrayList<>(splits);
		for (int i = 1; i <= splits; i++) {
			File outputFile = new File(String.format("%s.%s.FILTERED", filename, i));
			FileOutputStream fos = new FileOutputStream(outputFile);
			fileChannels.add(fos.getChannel());
		}
	}

	private void setOutputFileChannel(Long seenNum) {
		int index = 0;
		if (seenNum != null) {
			index = seenNum.intValue() % splits;
		}
		channel = fileChannels.get(index);
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
				Long seenconnectionnum = (Long) obj.get("seenconnectionnum");
				// logger.debug("seen: " + seenconnectionnum);

				setOutputFileChannel(seenconnectionnum);

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

				if (header != null) {
					int opcode = (Integer) header.get("opcode");
					incrementOpcodeSeenCount(opcode);
					ByteBufferBsonInput bsonInput = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bodyBytes)));
					BsonBinaryReader reader = new BsonBinaryReader(bsonInput);

					parsedHeader = MessageHeader.parse(bsonInput);

					// logger.debug("opcode: " + opcode + ", headerOpcode: " + headerOpcode);

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
						// logger.debug(String.format("Compressed, originalOpcode: %s", opcode));
						// Dumb hack, just double count the compressed / uncompressed opcode
						incrementOpcodeSeenCount(opcode);
						int uncompressedSize = bsonInput.readInt32();
						byte compressorId = bsonInput.readByte();

						// logger.debug("compressorId: " + compressorId);

						int position = bsonInput.getPosition();
						int remaining = parsedHeader.getMessageLength() - position;
						byte[] compressed = new byte[remaining];

						bsonInput.readBytes(compressed);
						byte[] uncompressed = Snappy.uncompress(compressed);
						//logger.debug(String.format("compressed.length: %s, uncompressedSize: %s,uncompressed.length: %s", 
						//		compressed.length, uncompressedSize,uncompressed.length));

						if (opcode == 2013) {
							ByteBufferBsonInput bi = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(uncompressed)));
							BsonBinaryReader r = new BsonBinaryReader(bi);
							process2013(bi, r, channel, uncompressed.length);
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

						// System.out.println("2004: " + commandDoc);
						Document queryCommand = (Document) commandDoc.get("$query");
						if (queryCommand != null) {
							commandDoc = queryCommand;
						}

						commandDoc.remove("projection");
						BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
						BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
						documentCodec.encode(tmpWriter, commandDoc, encoderContext);
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

						documentCodec.encode(writer, commandDoc, encoderContext);

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
						} else if (!command.equals("find")) {
							// logger.debug(command);
						}

						BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
						BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
						documentCodec.encode(tmpWriter, commandDoc, encoderContext);
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

						documentCodec.encode(writer, commandDoc, encoderContext);

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

						process2013(bsonInput, reader, channel, parsedHeader.getMessageLength());

					} else {
						logger.debug("Header was null, WTF?");
					}
				}
			}

			// count++;

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

	private void transcode2013(Document commandDoc, String collectionName) throws IOException {
		BasicOutputBuffer tmpBuff = new BasicOutputBuffer();
		BsonBinaryWriter tmpWriter = new BsonBinaryWriter(tmpBuff);
		documentCodec.encode(tmpWriter, commandDoc, encoderContext);
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

		documentCodec.encode(writer, commandDoc, encoderContext);

		int size1 = writer.getBsonOutput().getPosition();
		header.put("messagelength", size1);
		header.put("opcode", 2004);
		// System.out.println("obj: " + obj);
		raw.put("body", rawOut.toByteArray());
		ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
		channel.write(buffer);
		written++;
	}

	/**
	 * Note this implementation assumes that the header has already been consumed.
	 */
	private void process2013(ByteBufferBsonInput bsonInput, BsonBinaryReader reader, FileChannel channel, int messageLength) throws IOException {
		documents.clear();
		int kindZeroCount = 0;
		int count = 0;
		int flags = bsonInput.readInt32();
		boolean moreSections = true;
		String databaseName = null;
		Document commandDoc = null;
		
		Section currentSection = null;
		while (moreSections) {
			
			byte kindByte = bsonInput.readByte();
			currentSection = new Section(kindByte);
			count++;
			if (kindByte == 0) {
				kindZeroCount++;
				
				commandDoc = documentCodec.decode(reader, DecoderContext.builder().build());
				moreSections = messageLength > bsonInput.getPosition();

				databaseName = commandDoc.getString("$db");
				if (databaseName == null || databaseName.equals("local")
						|| databaseName.equals("admin")
						|| commandDoc.containsKey("getMore") 
						|| commandDoc.containsKey("ping")) {
					return;
				}

				commandDoc.remove("lsid");
				commandDoc.remove("$db");
				commandDoc.remove("$readPreference");
				commandDoc.remove("txnNumber");
				
				currentSection.setDocument(commandDoc);
				documents.addFirst(commandDoc);

			} else if (kindByte == 1) {
				int x0 = bsonInput.getPosition();
				try {

					int size = bsonInput.readInt32();
					String messageIdentifier = bsonInput.readCString();
					
					int x1 = bsonInput.getPosition();
					int remaining = size - (x1 -x0);
					
					if (messageIdentifier == null) {
						logger.warn("null messageIdentifier");
						return;
					}
					
					currentSection.setSize(size);
					currentSection.setMessageIdentifier(messageIdentifier);
					
					int docCount = 0;
					int currentSize;
					do {
						int p0 = bsonInput.getPosition();

						byte[] mb = new byte[remaining];
						bsonInput.readBytes(mb);
						BsonBinaryReader r2 = new BsonBinaryReader(ByteBuffer.wrap(mb));
						
						
						
						Document d = documentCodec.decode(r2, DecoderContext.builder().build());
						documents.add(d);
						currentSection.addDocument(d);
						int p1 = bsonInput.getPosition();
						currentSize = p1 - p0;
						docCount++;
					} while (currentSize < remaining);
					
					if (docCount > 1) {
						logger.debug("***** docCount: " + docCount);
					}
					
					moreSections = messageLength > bsonInput.getPosition();
				} catch (BsonSerializationException bse) {
					Integer req = (Integer) header.get("requestid");
					System.out.println("*** req: " + req + ",  pos: " + bsonInput.getPosition()
							+ ", len: " + parsedHeader.getMessageLength());

					bse.printStackTrace();

				}

			} else {
				//logger.warn("Invalid 2013 kind byte, ignoring op, kind=", + kindByte);
				moreSections = false;
			}

		}
		
		if (kindZeroCount != 1) {
			logger.error("Invalid kindZeroCount {}", kindZeroCount);
			return;
		}
		
		
		BasicOutputBuffer rawOut = new BasicOutputBuffer();
		BsonBinaryWriter writer = new BsonBinaryWriter(rawOut);
	
		
		rawOut.writeInt(0); // dummy length
		rawOut.writeInt(parsedHeader.getRequestId());
		rawOut.writeInt(parsedHeader.getResponseTo());
		rawOut.writeInt(2013);
		rawOut.writeInt(0); // flag bits
		int position = 0;
		for (Document document : documents) {
			if (position == 0) {
				rawOut.writeByte(0); // kind byte
			} else {
				rawOut.writeByte(1); // kind byte 
			}
			int beforeSize = rawOut.getSize();
			documentCodec.encode(writer, document, encoderContext);
			int afterSize = rawOut.getSize();
			int docSize = afterSize - beforeSize;
			//logger.debug("doc size: " + docSize);
		}
		//documentCodec.encode(writer, commandDoc, encoderContext);

		int size1 = writer.getBsonOutput().getPosition();
		header.put("messagelength", size1);
		header.put("opcode", 2013);
		// System.out.println("obj: " + obj);
		raw.put("body", rawOut.toByteArray());
		ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
		channel.write(buffer);
		written++;
		
		
			
			
		if (commandDoc.containsKey("insert")) {
			String collName = commandDoc.getString("insert");
			this.transcode2013(commandDoc, collName);
		} else if (commandDoc.containsKey("update")) {
			//commandDoc.put("updates", Arrays.asList(d1));
		} else if (commandDoc.containsKey("find")) {
			String collName = commandDoc.getString("find");
			transcode2013(commandDoc, collName);
			
		} else if (commandDoc.containsKey("aggregate")) {
			String collName = commandDoc.getString("aggregate");
			transcode2013(commandDoc, collName);
			
		} else if (commandDoc.containsKey("count")) {
			String collName = commandDoc.getString("count");
			transcode2013(commandDoc, collName);
		} else if (commandDoc.containsKey("delete")) {
			String collName = commandDoc.getString("delete");
			transcode2013(commandDoc, collName);
		}  else if (commandDoc.containsKey("getMore") || commandDoc.containsKey("ping")) {
			
			return;
		} else {
			logger.warn("ignored command: " + commandDoc);
		}
		
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

		options.addOption(
				OptionBuilder.withArgName("number of output files split").hasArgs().withLongOpt("split").create("s"));

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
