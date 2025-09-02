package com.mongodb.shardsync.command;

import picocli.CommandLine.Option;

/**
 * Advanced connection configuration options that are rarely used.
 * These options provide fine-grained control over cluster connections.
 */
public class AdvancedConnectionMixin {
    
    // Pattern-based connection options
    @Option(names = {"--sourceUriPattern"}, 
            description = "Source cluster connection URI pattern",
            hidden = true)
    public String sourceUriPattern;
    
    @Option(names = {"--destUriPattern"}, 
            description = "Destination cluster connection URI pattern",
            hidden = true)
    public String destUriPattern;
    
    // SSL options
    @Option(names = {"--sourceRsSsl"}, 
            description = "Use SSL for source replica set connections",
            hidden = true)
    public Boolean sourceRsSsl;
    
    // Manual replica set specification
    @Option(names = {"--sourceRsManual"}, 
            arity = "0..*", 
            description = "Manually specify source replica sets",
            hidden = true)
    public String[] sourceRsManual;
    
    @Option(names = {"--destRsManual"}, 
            arity = "0..*", 
            description = "Manually specify destination replica sets",
            hidden = true)
    public String[] destRsManual;
    
    // Regex patterns for replica sets
    @Option(names = {"--sourceRsRegex"}, 
            description = "Source replica set regex pattern",
            hidden = true)
    public String sourceRsRegex;
    
    @Option(names = {"--destRsRegex"}, 
            description = "Destination replica set regex pattern",
            hidden = true)
    public String destRsRegex;
    
    // Config server and pattern options
    @Option(names = {"--destCsrs"}, 
            description = "Destination config server replica set URI",
            hidden = true)
    public String destCsrsUri;
    
    @Option(names = {"--sourceRsPattern"}, 
            description = "Source replica set pattern",
            hidden = true)
    public String sourceRsPattern;
    
    @Option(names = {"--destRsPattern"}, 
            description = "Destination replica set pattern",
            hidden = true)
    public String destRsPattern;
}