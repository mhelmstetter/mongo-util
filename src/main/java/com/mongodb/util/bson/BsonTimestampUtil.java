package com.mongodb.util.bson;

import org.bson.BsonTimestamp;

public class BsonTimestampUtil {

	public static void main(String[] args) {
		if (args.length > 0) {
			if (args[0].contains(",")) {
				String[] tsParts = args[0].split(",");
				int seconds = Integer.parseInt(tsParts[0]);
				int increment = Integer.parseInt(tsParts[1]);
				System.out.println(new BsonTimestamp(seconds, increment));
				
			} else {
				System.out.println("No comma found in argument");
			}
		} else {
			System.out.println("usage: BsonTimestampUtil <seconds>,<increment>");
		}
		
	}

}
