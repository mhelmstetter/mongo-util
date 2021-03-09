package com.mongodb.util.bson;

import java.util.Date;

import org.bson.BsonTimestamp;

public class BsonTimestampParser {

	public static void main(String[] args) {
		if (args.length > 0) {
			long ts = Long.parseLong(args[0]);
			BsonTimestamp bson = new BsonTimestamp(ts);
			System.out.println(bson);
			int epochSeconds = bson.getTime();
			System.out.println(new Date(epochSeconds*1000));
		} else {
			System.out.println("usage: BsonTimestampParser <long>");
		}
		
	}

}
