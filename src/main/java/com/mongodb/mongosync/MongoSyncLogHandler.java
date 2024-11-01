package com.mongodb.mongosync;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.exec.LogOutputStream;

public class MongoSyncLogHandler extends LogOutputStream {
    private MongoSyncEventListener listener;
    
    private static final String[] READY_FOR_PAUSE_PHRASES = new String[] {
            "READY_FOR_PAUSE"
    };
    
    private static final String REGEX_PREFIX = ".*(?:";
    private static final String REGEX_SUFFIX = ").*";
    private Pattern readyForPauseRegex;
    
    private PrintWriter writer;

    public MongoSyncLogHandler(MongoSyncEventListener listener, String id, File logPath) throws IOException {
        super();
        this.listener = listener;
        readyForPauseRegex = constructRegexContainingPhrases(READY_FOR_PAUSE_PHRASES);
        writer = new PrintWriter(new FileWriter(new File(logPath, id + ".log")));
    }
    
    private void matcher(String line) {
    	if (listener != null && readyForPauseRegex.matcher(line).matches() ) {
    		listener.readyForPauseAfterCollectionCreation();
    	}
    }
    
    @Override
    protected void processLine(String line, int logLevel) {
    	writer.println(line);
    	writer.flush();
        matcher(line);
    }

    /* Form a regex that matches any of an array of phrases */
    private Pattern constructRegexContainingPhrases(String[] phrases) {
        StringBuilder sb = new StringBuilder(REGEX_PREFIX);
        Iterator<String> matchIter = Arrays.stream(phrases).iterator();
        while (matchIter.hasNext()) {
            sb.append("\\b");
            sb.append(matchIter.next());
            if (matchIter.hasNext()){
                sb.append("|");
            }
        }
        sb.append(REGEX_SUFFIX);
        return Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
    }

    public MongoSyncEventListener getListener() {
        return listener;
    }
}
