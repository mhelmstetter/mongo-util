package com.mongodb.mongoreplay;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;

public class MongoReplayStats {

    private final BasicBSONEncoder encoder;

    public MongoReplayStats() {
        this.encoder = new BasicBSONEncoder();
    }

    public void filterFile(String filename) throws FileNotFoundException {

        File file = new File(filename);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        HashMap<Integer, AtomicInteger> atomicCounter = new HashMap<Integer, AtomicInteger>();

        BSONDecoder decoder = new BasicBSONDecoder();
        int count = 0;
        int written = 0;
        try {
            while (inputStream.available() > 0) {
                BSONObject obj = decoder.readObject(inputStream);
                if (obj == null) {
                    break;
                }

                BSONObject raw = (BSONObject) obj.get("rawop");
                BSONObject header = (BSONObject) raw.get("header");
                if (header != null) {
                    Integer opcode = (Integer) header.get("opcode");
                    
                    AtomicInteger frequency = atomicCounter.get(opcode);
                    if (frequency != null) {
                        frequency.incrementAndGet();
                    } else {
                        atomicCounter.put(opcode, new AtomicInteger(1));
                    }
                }

                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
            }
        }
        System.err.println(String.format("%s objects read, %s filtered objects written", count, written));
        
        Iterator<Entry<Integer, AtomicInteger>> it = atomicCounter.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, AtomicInteger> pair = it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
        }
    }

    public static void main(String args[]) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("Expected <bson filename> argument");
        }
        String filename = args[0];
        MongoReplayStats filter = new MongoReplayStats();
        filter.filterFile(filename);

    }

}
