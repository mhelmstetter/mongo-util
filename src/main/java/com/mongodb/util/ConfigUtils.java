package com.mongodb.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.Configuration;

public class ConfigUtils {
	
	public static String[] getConfigValues(CommandLine line, Configuration props, String key) {
		if (line.hasOption(key)) {
			return line.getOptionValues(key);
		} else {
			return props.getStringArray(key);
		}
	}
	
	public static String getConfigValue(CommandLine line, Configuration props, String key, String defaultValue) {
        return defaultValue != null && defaultValue.length() > 0 ?
                line.getOptionValue(key, props.getString(key, defaultValue)) :
                line.getOptionValue(key, props.getString(key));
    }
    
    public static String getConfigValue(CommandLine line, Configuration props, String key) {
        return getConfigValue(line, props, key, null);
    }

}
