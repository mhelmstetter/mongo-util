package com.mongodb.util;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "fileSetDiff", mixinStandardHelpOptions = true, version = "fileSetDiff 0.1")
public class FileSetDifference implements Callable<Integer> {

	private static Logger logger = LoggerFactory.getLogger(FileSetDifference.class);

	
	@Option(names = { "--statsFile" }, required = true)
	private String statsFile;
	
	@Option(names = { "--oplogFile" }, required = true)
	private String oplogFile;
	

	private final Map<String,Long> opCountMap = new LinkedHashMap<>();


	@Override
	public Integer call() throws Exception {
		
		Path oplogPath = Paths.get(oplogFile);
		try (BufferedReader reader = Files.newBufferedReader(oplogPath)) {
			String line;
			while ((line = reader.readLine()) != null) {
				
				String[] toks = line.split("\\s+");
				String ns = toks[0];
				String opCountStr = toks[2];
				Long opCount = Long.parseLong(opCountStr);
				opCountMap.put(ns, opCount);
			}
		}
		
		
		Path statsPath = Paths.get(statsFile);
		
		try (BufferedReader reader = Files.newBufferedReader(statsPath)) {
	
			String line;
			while ((line = reader.readLine()) != null) {
				
				String[] toks = line.split(",");
				String db = toks[0];
				String coll = toks[1];
				String ns = db + "." + coll;
				
				String size = toks[2];
				
				if (!opCountMap.containsKey(ns)) {
					System.out.println(ns + "\\t" + size);
				}
				
			}
			
		}
		return 0;
	}

	

	public static void main(String... args) {
		int exitCode = new CommandLine(new FileSetDifference()).execute(args);
		System.exit(exitCode);
	}

}
