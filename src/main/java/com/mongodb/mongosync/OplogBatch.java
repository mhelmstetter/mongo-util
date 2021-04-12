package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.WriteModel;

public class OplogBatch {
	
	
	//private Map<Integer, BsonValue> orderedIndexToIdMap;
	//private Map<Integer, BsonValue> unorderedIndexToIdMap;
	
	private List<WriteModel<BsonDocument>> orderedWriteModels;
	private List<WriteModel<BsonDocument>> unorderedWriteModels;

	private Map<BsonValue, WriteModel<BsonDocument>> idsMap;
	
	public OplogBatch(int batchSize) {
		this.orderedWriteModels = new ArrayList<>(batchSize);
		//this.orderedIndexToIdMap = new HashMap<>(batchSize);
		this.unorderedWriteModels = new ArrayList<>(batchSize);
		//this.unorderedIndexToIdMap = new HashMap<>(batchSize);
		idsMap = new HashMap<>(batchSize);
	}

	public int size() {
		return orderedWriteModels.size() + unorderedWriteModels.size();
	}
	
	public void addWriteModel(WriteModel<BsonDocument> model, BsonValue id) {
		
		boolean existing = idsMap.containsKey(id);
		int index;
		
		// if the entry is already existing, it is a duplicate needs to be ordered operations
		if (existing) {
			WriteModel<BsonDocument> lastElement = idsMap.get(id);
			// if it's null there's already more than 1
			// otherwise the value is the index of the last occurrence
			if (lastElement != null) {
				unorderedWriteModels.remove(lastElement);
				orderedWriteModels.add(lastElement);
				idsMap.put(id, null);
			}
			orderedWriteModels.add(model);
			index = orderedWriteModels.size() - 1;
			//orderedIndexToIdMap.put(index, id);
		} else {
			unorderedWriteModels.add(model);
			index = unorderedWriteModels.size() - 1;
			//unorderedIndexToIdMap.put(index, id);
			idsMap.put(id, model);
		}
	}
	
	public void removeWriteModel(WriteModel<BsonDocument> model, boolean ordered) {
		BsonValue id = null;
		if (ordered) {
			//id = orderedIndexToIdMap.remove(model);
			orderedWriteModels.remove(model);
		} else {
			//id = unorderedIndexToIdMap.remove(index);
			unorderedWriteModels.remove(model);
		}
	}
	
	public void clearUnordered() {
		unorderedWriteModels.clear();
	}
	
	public void clear() {
		unorderedWriteModels.clear();
		//unorderedIndexToIdMap.clear();
		orderedWriteModels.clear();
		//orderedIndexToIdMap.clear();
		idsMap.clear();
	}

	public List<WriteModel<BsonDocument>> getOrderedWriteModels() {
		return orderedWriteModels;
	}
	
	public List<WriteModel<BsonDocument>> getUnorderedWriteModels() {
		return unorderedWriteModels;
	}

}
