package com.mongodb.mongomirror;

import org.apache.commons.exec.LogOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

public class MongoMirrorLogHandler extends LogOutputStream {
    private MongoMirrorEventListener listener;
    private static final String[] ERR_MATCH_PHRASES = new String[] {
            "fail", "error", "unable", "could not"
    };
    private static final String REGEX_PREFIX = ".*(?:";
    private static final String REGEX_SUFFIX = ").*";
    private Pattern errMatchRegex;
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public MongoMirrorLogHandler(MongoMirrorEventListener listener){
        super();
        this.listener = listener;
        errMatchRegex = constructRegexContainingPhrases(ERR_MATCH_PHRASES);
    }
    @Override
    protected void processLine(String line, int logLevel) {
        logger.info(line);
        if (errMatchRegex.matcher(line).matches()) {
            listener.procLoggedError(line);
        }
    }

    /* Form a regex that matches any of an array of phrases */
    private Pattern constructRegexContainingPhrases(String[] phrases) {
        StringBuilder sb = new StringBuilder(REGEX_PREFIX);
        Iterator<String> matchIter = Arrays.stream(phrases).iterator();
        while (matchIter.hasNext()) {
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
