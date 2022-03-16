/*
 * Copyright (c)2005-2008 Mark Logic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */

package com.mongodb.util;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class Timer {

    public static final int BYTES_PER_KILOBYTE = 1024;

    public static final int MILLISECONDS_PER_SECOND = 1000;

    public static final int NANOSECONDS_PER_MICROSECOND = MILLISECONDS_PER_SECOND;

    public static final int MICROSECONDS_PER_MILLISECOND = MILLISECONDS_PER_SECOND;

    public static final int NANOSECONDS_PER_MILLISECOND = NANOSECONDS_PER_MICROSECOND
            * MICROSECONDS_PER_MILLISECOND;

    public static final int NANOSECONDS_PER_SECOND = NANOSECONDS_PER_MILLISECOND
            * MILLISECONDS_PER_SECOND;

    private volatile AtomicInteger errors;

    private long duration = -1;

    private volatile ArrayList<TimedEvent> events = new ArrayList<TimedEvent>();

    private long start;

    private volatile AtomicInteger eventCount;

    public Timer() {
        start = System.nanoTime();
        eventCount = new AtomicInteger(0);
        errors = new AtomicInteger(0);
    }

    /**
     * @return
     */
    public AtomicInteger getEventCount() {
        return eventCount;
    }

    /**
     * @return
     */
    public long getErrorCount() {
        return errors.get();
    }

    /**
     * @return
     */
    public long getDuration() {
        if (duration < 0)
            return (System.nanoTime() - start);

        return duration;
    }


    /**
     * @return
     */
    public long getStart() {
        return start;
    }


    public double getEventsPerSecond() {
        // events per second
        return eventCount.get() / getDurationSeconds();
    }

    /**
     * @param l
     */
    public long stop() {
        return stop(System.nanoTime());
    }

    /**
     * @param l
     */
    public synchronized long stop(long l) {
        if (duration < 0) {
            duration = l - start;
        }

        return duration;
    }

    /**
     * 
     */
    public void incrementEventCount() {
        eventCount.incrementAndGet();
    }
    
    public void incrementErrorCount() {
        errors.incrementAndGet();
    }

    /**
     * @return
     */
    public double getDurationMilliseconds() {
        // ns to seconds
        return ((double) getDuration())
                / ((double) NANOSECONDS_PER_MILLISECOND);
    }

    /**
     * @return
     */
    public double getDurationSeconds() {
        // ns to seconds
        return ((double) getDuration())
                / ((double) NANOSECONDS_PER_SECOND);
    }

    public String getProgressMessage() {
        return Math.round(getEventsPerSecond()) + " tps";
    }

}
