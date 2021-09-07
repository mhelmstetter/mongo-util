package com.mongodb.mongoreplay.opcodes;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

public class Section {
	
	public Section(byte payloadType) {
		this.payloadType = payloadType;
	}

	// aka "kind byte" 0 or 1
	private byte payloadType;
	
	private Document document;
	
	private String messageIdentifier; 
	
	private int size;
	
	private List<Document> documents;

	public byte getPayloadType() {
		return payloadType;
	}

	public void setPayloadType(byte payloadType) {
		this.payloadType = payloadType;
	}

	public Document getDocument() {
		return document;
	}

	public void setDocument(Document document) {
		this.document = document;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public List<Document> getDocuments() {
		return documents;
	}
	
	public void addDocument(Document d) {
		if (documents == null) {
			documents = new ArrayList<>(1);
		}
		documents.add(d);
	}

	public String getMessageIdentifier() {
		return messageIdentifier;
	}

	public void setMessageIdentifier(String messageIdentifier) {
		this.messageIdentifier = messageIdentifier;
	}
	

}
