package com.mongodb.mongoreplay;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;

public class MongoReplayFilter {

    private final BasicBSONEncoder encoder;

    public MongoReplayFilter() {
        this.encoder = new BasicBSONEncoder();
    }

    public void filterFile(String filename) throws FileNotFoundException {

        File file = new File(filename);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));

        File outputFile = new File(filename + ".FILTERED");
        FileOutputStream fos = null;

        BSONDecoder decoder = new BasicBSONDecoder();
        int count = 0;
        int written = 0;
        try {
            fos = new FileOutputStream(outputFile);
            FileChannel channel = fos.getChannel();
            while (inputStream.available() > 0) {
                BSONObject obj = decoder.readObject(inputStream);
                if (obj == null) {
                    break;
                }

                BSONObject raw = (BSONObject) obj.get("rawop");
                BSONObject header = (BSONObject) raw.get("header");
                if (header != null) {
                    Integer opcode = (Integer) header.get("opcode");
                    if (opcode.equals(2004)) {
                        ByteBuffer buffer = ByteBuffer.wrap(encoder.encode(obj));
                        channel.write(buffer);
                        written++;
                    } else {
                        System.out.println(opcode);
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
    }

    public static void main(String args[]) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("Expected <bson filename> argument");
        }
        String filename = args[0];
        MongoReplayFilter filter = new MongoReplayFilter();
        filter.filterFile(filename);

    }

}
