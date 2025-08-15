package com.mongodb.shardsync.command;

import picocli.CommandLine.Option;

/**
 * Mixin for Atlas API related options that can be reused across commands
 */
public class AtlasOptionsMixin {
    
    @Option(names = {"--atlasApiPublicKey"}, description = "Atlas API public key")
    public String atlasApiPublicKey;
    
    @Option(names = {"--atlasApiPrivateKey"}, description = "Atlas API private key")
    public String atlasApiPrivateKey;
    
    @Option(names = {"--atlasProjectId"}, description = "Atlas project ID")
    public String atlasProjectId;
}