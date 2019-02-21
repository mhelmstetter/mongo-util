package com.mongodb.mongoreplay;

import static com.mongodb.mongoreplay.CommandType.READ;
import static com.mongodb.mongoreplay.CommandType.WRITE;

public enum Command {
    
    FIND(READ),
    
    INSERT(WRITE), UPDATE(WRITE), GETMORE(WRITE), AGGREGATE(READ);
    
    private CommandType commandType;
    
    private Command(CommandType commandType) {
        this.commandType = commandType;
    }

    public CommandType getCommandType() {
        return commandType;
    }
    
    public boolean isRead() {
        return commandType == READ;
    }

}
