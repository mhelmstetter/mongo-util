package com.mongodb.mongomirror;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.exec.LogOutputStream;

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

    public MongoMirrorLogHandler(MongoMirrorEventListener listener, String mongomirrorId) throws IOException {
        super();
        this.listener = listener;
        errMatchRegex = constructRegexContainingPhrases(ERR_MATCH_PHRASES);
        exclusionRegex = constructRegexContainingPhrases(EXCLUSION_PHRASES);
        writer = new PrintWriter(new FileWriter(new File(mongomirrorId + ".log")));
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
