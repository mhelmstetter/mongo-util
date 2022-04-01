package com.mongodb.util;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;

import com.mongodb.ConnectionString;

public class MaskUtil {
	
	
	public static String maskConnectionString(final ConnectionString cs) {
		String csStr = cs.getConnectionString();
		String after = StringUtils.substringAfter(csStr, "@");
		if (after == null || after.isEmpty()) {
			return csStr;
		}
		String before = StringUtils.substringBefore(csStr, "://");
		return before + "://" + "*****:*****@" + after;
	}
	
	public static String maskCommandLine(final CommandLine commandLine, String[] keys) {
		StringBuilder sb = new StringBuilder();
		
		String[] cmdLineStrings = commandLine.toStrings();
		
		
		int i = 0;
		for (String c : cmdLineStrings) {
			boolean found = false;
		
			for (String word : keys) {
				String before = StringUtils.substringBefore(c, "=");
				if (before.equals(word)) {
					found = true;
		            break;
				}
		    }
			
			if (found) {
				sb.append(StringUtils.substringBefore(c, "="));
				sb.append("=*****");
			} else {
				sb.append(c);
			}
			
			
			i++;
			if (i < cmdLineStrings.length) {
				sb.append(", ");
			}
			
		}
		return sb.toString();
	}
	
	

}
