package com.mongodb.custombalancer.commands;

import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import com.mongodb.custombalancer.CustomBalancerApp;

@Command(name = "manual-move", description = "Manually move all chunks with a specific prefix to a target shard")
public class ManualMoveCommand implements Callable<Integer> {

    @ParentCommand
    private CustomBalancerApp parent;

    @Option(names = {"--prefix"}, description = "Prefix value as JSON (e.g., '{\"customerId\": 12345}')", required = true)
    private String prefix;

    @Option(names = {"--targetShard"}, description = "Target shard ID (if not specified, moves to least loaded shard)")
    private String targetShard;

    @Option(names = {"--collections"}, split = ",", description = "Comma-separated list of collections (optional)")
    private String[] collections;

    @Override
    public Integer call() throws Exception {
        System.out.println("Manual move command not yet implemented");
        System.out.println("Prefix: " + prefix);
        System.out.println("Target shard: " + targetShard);
        return 0;
    }
}