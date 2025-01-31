package com.mongodb.mongosync;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.exec.LogOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSyncLogHandler extends LogOutputStream {
	
	protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);
	
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
        
        File logFile = new File(logPath, id + ".log");
        if (logFile.exists()) {
            // Get current date and time
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd:HH:mm").format(new Date());
            // Create a new file name with the date appended
            File renamedFile = new File(logPath, id + "_" + timeStamp + ".log");
            if (logFile.renameTo(renamedFile)) {
                logger.debug("Renamed existing log file to: {}", renamedFile.getName());
            } else {
                logger.warn("Failed to rename existing log file.");
            }
        }
        
        writer = new PrintWriter(new FileWriter(logFile));
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
