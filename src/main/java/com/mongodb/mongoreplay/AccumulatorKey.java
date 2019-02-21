package com.mongodb.mongoreplay;

public class AccumulatorKey {

    private String dbName;
    private String collName;
    private String shape;
    private Command command;

    public AccumulatorKey(String dbName, String collName, Command command, String shape) {
        this.dbName = dbName;
        this.collName = collName;
        this.shape = shape;
        this.command = command;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((collName == null) ? 0 : collName.hashCode());
        result = prime * result + ((command == null) ? 0 : command.hashCode());
        result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
        result = prime * result + ((shape == null) ? 0 : shape.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AccumulatorKey other = (AccumulatorKey) obj;
        if (collName == null) {
            if (other.collName != null)
                return false;
        } else if (!collName.equals(other.collName))
            return false;
        if (command != other.command)
            return false;
        if (dbName == null) {
            if (other.dbName != null)
                return false;
        } else if (!dbName.equals(other.dbName))
            return false;
        if (shape == null) {
            if (other.shape != null)
                return false;
        } else if (!shape.equals(other.shape))
            return false;
        return true;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollName() {
        return collName;
    }

    public String getShape() {
        return shape;
    }

    public Command getCommand() {
        return command;
    }

    public String getNamespace() {
        return dbName + "." + collName;
    }


}
