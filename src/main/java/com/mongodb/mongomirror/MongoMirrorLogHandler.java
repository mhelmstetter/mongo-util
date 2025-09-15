package com.mongodb.mongomirror;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.exec.LogOutputStream;

import com.mongodb.util.RollingLogFile;

public class MongoMirrorLogHandler extends LogOutputStream {
    private MongoMirrorEventListener listener;
    private static final String[] ERR_MATCH_PHRASES = new String[] {
            "fail", "error", "unable", "could not"
    };
    
    // don't alert if we match an error that also matches any exclusion
    private final static String[] EXCLUSION_PHRASES = new String[] {"warning", "Recoverable error", "transient error"};
    
    private static final String REGEX_PREFIX = ".*(?:";
    private static final String REGEX_SUFFIX = ").*";
    private Pattern errMatchRegex;
    private Pattern exclusionRegex;
    
    private PrintWriter writer;

    public MongoMirrorLogHandler(MongoMirrorEventListener listener, String mongomirrorId, String logPath) throws IOException {
        super();
        this.listener = listener;
        errMatchRegex = constructRegexContainingPhrases(ERR_MATCH_PHRASES);
        exclusionRegex = constructRegexContainingPhrases(EXCLUSION_PHRASES);
        
        // Determine the log file path
        String logFileName;
        if (logPath != null && !logPath.trim().isEmpty()) {
            logFileName = Paths.get(logPath, mongomirrorId + ".log").toString();
        } else {
            logFileName = mongomirrorId + ".log";
        }
        
        // Use RollingLogFile for rolling log file functionality with compression
        RollingLogFile rollingFile = new RollingLogFile(logFileName, true);
        OutputStream outputStream = rollingFile.openForWrite();
        writer = new PrintWriter(new OutputStreamWriter(outputStream));
    }
    
    private void matcher(String line) {
    	if (listener != null && errMatchRegex.matcher(line).matches() && !exclusionRegex.matcher(line).matches()) {
    		listener.procLoggedError(line);
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

    public MongoMirrorEventListener getListener() {
        return listener;
    }
}
