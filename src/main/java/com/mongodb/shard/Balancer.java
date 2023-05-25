package com.mongodb.shard;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "balancer", mixinStandardHelpOptions = true, version = "balancer 0.1", description = "Custom mongodb shard balancer")
public class Balancer implements Callable<Integer> {

	private static Logger logger = LoggerFactory.getLogger(Balancer.class);

	@Option(names = { "--uri" }, description = "mongodb uri connection string", required = true)
	private String uri;
	
	@Option(names = { "--sourceShards" }, required = false)
	private String[] sourceShards;
	
	@Option(names = { "--mode" }, required = false)
	private String mode;
	
	@Option(names = { "--ns" }, required = false)
	private String[] namespaces;
	
	@Option(names = { "--nsFile" }, required = false)
	private String namespaceFile;
	
	@Option(names="--dryRun", arity="0") 
	boolean dryRun;


	@Override
	public Integer call() throws Exception {
		
		if (namespaceFile != null) {
			readNamespaceFile();
		}

		if (mode != null && mode.equals("manual")) {
			ManualBalancingStrategy balancer = new ManualBalancingStrategy(uri);
			balancer.setNamespaces(namespaces);
			if (sourceShards != null) {
				balancer.setSourceShards(sourceShards);
			}
			balancer.balance();
			
		} else if (mode != null && mode.equals("v3")) {
			V3BalancingStrategy balancer = new V3BalancingStrategy(uri);
			balancer.setNamespaces(namespaces);
			if (sourceShards != null) {
				balancer.setSourceShards(sourceShards);
			}
			balancer.setDryRun(dryRun);
			balancer.balance();
		} else {
			HighLowBalancingStrategy balancer = new HighLowBalancingStrategy(uri);
			balancer.balance();
		}
		

		return 0;
	}

	private void readNamespaceFile() throws IOException {
		Path path = Paths.get(namespaceFile);
		List<String> ns = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(path)) {
			String line;
			while ((line = reader.readLine()) != null) {
				ns.add(line.trim());
			}
		}
		this.namespaces = ns.stream().toArray(String[]::new);
	}
	

	public static void main(String... args) {
		int exitCode = new CommandLine(new Balancer()).execute(args);
		System.exit(exitCode);
	}

}
