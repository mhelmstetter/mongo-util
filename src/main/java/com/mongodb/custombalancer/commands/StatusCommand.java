package com.mongodb.custombalancer.commands;

import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import com.mongodb.custombalancer.CustomBalancerApp;

@Command(name = "status", description = "Show current balancer status and recent migrations")
public class StatusCommand implements Callable<Integer> {

    @ParentCommand
    private CustomBalancerApp parent;

    @Override
    public Integer call() throws Exception {
        System.out.println("Status command not yet implemented");
        return 0;
    }
}