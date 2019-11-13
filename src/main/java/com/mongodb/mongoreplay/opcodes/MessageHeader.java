package com.mongodb.mongoreplay.opcodes;

import org.bson.io.ByteBufferBsonInput;

public class MessageHeader {
    
    private int messageLength;
    private int requestId;
    private int responseTo;
    private int headerOpcode;
    
    
    public MessageHeader(int messageLength, int requestId, int responseTo, int headerOpcode) {
        this.messageLength = messageLength;
        this.requestId = requestId;
        this.responseTo = responseTo;
        this.headerOpcode = headerOpcode;
    }

    public static MessageHeader parse(ByteBufferBsonInput bsonInput) {
        int messageLength = bsonInput.readInt32();
        int requestId = bsonInput.readInt32();
        int responseTo = bsonInput.readInt32();
        int headerOpcode = bsonInput.readInt32();
        return new MessageHeader(messageLength, requestId, responseTo, headerOpcode);
    }

    public int getMessageLength() {
        return messageLength;
    }

    public int getRequestId() {
        return requestId;
    }

    public int getResponseTo() {
        return responseTo;
    }

    public int getHeaderOpcode() {
        return headerOpcode;
    }

}
