package com.mongodb.custombalancer.commands;

import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import com.mongodb.custombalancer.CustomBalancerApp;

@Command(name = "balance", description = "Run the custom balancer to balance chunks across shards")
public class BalanceCommand implements Callable<Integer> {

    @ParentCommand
    private CustomBalancerApp parent;

    @Override
    public Integer call() throws Exception {
        System.out.println("Balance command not yet implemented");
        System.out.println("Source URI: " + parent.getSourceUri());
        return 0;
    }
}