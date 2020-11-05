package com.mongodb.schema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bson.BsonType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSharpClassWriter {
	
	private static Logger logger = LoggerFactory.getLogger(SchemaAnalyzer.class);

	private File outputFile;
	private PrintWriter writer;
	private String className;

	public CSharpClassWriter(String collectionName) throws IOException {
		className = collectionName.substring(0, 1).toUpperCase() + collectionName.substring(1);
		outputFile = new File(className + ".cs");
		writer = new PrintWriter(new FileWriter(outputFile));
	}

	public void writeClass(LinkedHashMap<String, BsonType> keyTypeMap) {

		writer.println(String.format("public class %s {", className));
		for (Map.Entry<String, BsonType> entry : keyTypeMap.entrySet()) {
			String fieldName = entry.getKey();
			String csharpType = getType(entry.getValue());
			writer.println(String.format("    public %s %s { get; set; }", csharpType, fieldName));
		}
		writer.println("}");
		writer.flush();
		writer.close();
		logger.debug(String.format("Write class file %s", outputFile.getAbsolutePath()));

	}

	private String getType(BsonType value) {
		switch (value) {
			case OBJECT_ID:
				return "ObjectId";
			case STRING:
				return "string";
			case DOUBLE:
				return "double";
			case DATE_TIME:
				return "DateTime";
			case BOOLEAN:
				return "bool";
			case INT32:
				return "int";
			case INT64:
				return "long";
			case BINARY:
				return "byte[]";
			default:
				return "string";
			}
	}

}
