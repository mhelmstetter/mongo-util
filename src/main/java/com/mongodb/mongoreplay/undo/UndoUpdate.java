package com.mongodb.mongoreplay.undo;

import org.bson.Document;

public class UndoUpdate {
    
    public Document query;
    
    public UndoUpdate(Document query) {
        this.query = query;
    }
    
    

}
