package com.mongodb.shardsync.command.drop;

import com.mongodb.shardsync.command.DropCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "indexes", description = "Drop indexes on destination")
public class DropIndexesCommand implements Callable<Integer> {
    
    private static Logger logger = LoggerFactory.getLogger(DropIndexesCommand.class);
    
    @CommandLine.ParentCommand
    private DropCommand parent;
    
    @Override
    public Integer call() throws Exception {
        // TODO: Implement drop indexes functionality
        logger.warn("Drop indexes functionality not yet implemented");
        System.out.println("Drop indexes functionality is not yet implemented");
        return 1;
    }
}