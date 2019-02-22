package com.mongodb.mongoreplay;

import static com.mongodb.mongoreplay.CommandType.READ;
import static com.mongodb.mongoreplay.CommandType.WRITE;

public enum Command {

    FIND(READ),

    INSERT(WRITE), UPDATE(WRITE), GETMORE(WRITE), AGGREGATE(READ), DELETE(WRITE), COUNT(READ), FIND_AND_MODIFY(WRITE);

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
