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
import java.util.Collections;
import java.util.Comparator;

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

    private volatile long errors = 0;

    private volatile long bytes = 0;

    private long duration = -1;

    private volatile ArrayList<TimedEvent> events = new ArrayList<TimedEvent>();

    private long start;

    private volatile long eventCount;

    public Timer() {
        start = System.nanoTime();
        eventCount = 0;
    }

    public void add(TimedEvent event) {
        add(event, true);
    }

    public void add(TimedEvent event, boolean _keepEvent) {
        // in case the user forgot to call stop(): note that bytes won't be
        // counted!
        event.stop();
        synchronized (events) {
            bytes += event.getBytes();
            if (event.isError()) {
                errors += event.getErrorCount();
            }
            if (_keepEvent) {
                events.add(event);
            }
            eventCount += event.getCount();
        }
    }

    /**
     * @param _timer
     */
    public void add(Timer _timer) {
        add(_timer, true);
    }

    /**
     * @param _timer
     * @param _keep
     */
    public void add(Timer _timer, boolean _keep) {
        _timer.stop();
        synchronized (events) {
            bytes += _timer.getBytes();
            errors += _timer.getErrorCount();
            if (_keep) {
                events.addAll(_timer.events);
            }
            eventCount += _timer.eventCount;
        }
    }

    /**
     * @return
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * @return
     */
    public long getEventCount() {
        return eventCount;
    }

    /**
     * @return
     */
    public long getSuccessfulEventCount() {
        return eventCount;
    }

    /**
     * @return
     */
    public long getErrorCount() {
        return errors;
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
    public long getMeanOfEvents() {
        if (eventCount < 1)
            return 0;

        long sum = 0;
        for (int i = 0; i < eventCount; i++) {
            sum += events.get(i).getDuration();
        }

        return Math.round((double) sum / eventCount);
    }

    /**
     * @return
     */
    public long getPercentileDuration(int p) {
        if (eventCount < 1)
            return 0;

        double size = eventCount;
        Comparator<TimedEvent> c = new TimedEventDurationComparator();
        Collections.sort(events, c);
        int pidx = (int) (p * size * .01);
        return events.get(pidx).getDuration();
    }

    /**
     * @return
     */
    public long getMaxDuration() {
        long max = 0;
        for (int i = 0; i < eventCount; i++)
            max = Math.max(max, events.get(i).getDuration());
        return max;
    }

    /**
     * @return
     */
    public long getMinDuration() {
        long min = Integer.MAX_VALUE;
        for (int i = 0; i < eventCount; i++)
            min = Math.min(min, events.get(i).getDuration());
        return min;
    }

    /**
     * @return
     */
    public long getMeanOverall() {
        return getDuration() / eventCount;
    }

    /**
     * @return
     */
    public long getStart() {
        return start;
    }

    /**
     * @return
     */
    public long getKiloBytes() {
        return (long) ((double) bytes / BYTES_PER_KILOBYTE);
    }

    public double getKilobytesPerSecond() {
        return ((double) bytes / BYTES_PER_KILOBYTE)
                / getDurationSeconds();
    }

    public double getEventsPerSecond() {
        // events per second
        return eventCount / getDurationSeconds();
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
        eventCount++;
    }

    /**
     * @param count
     */
    public void incrementEventCount(int count) {
        eventCount += count;
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

    public String getProgressMessage(boolean rawValues) {
        return (rawValues ? getBytes() + " B in " + getDurationSeconds()
                + " s, " : "")
                + Math.round(getEventsPerSecond())
                + " tps";
                //+ Math.round(getKilobytesPerSecond()) + " kB/s";
    }

    /**
     * @return
     */
    public String getProgressMessage() {
        return getProgressMessage(false);
    }

    /**
     * @return
     */
    public int getBytesPerSecond() {
        return (int) (bytes / getDurationSeconds());
    }

}
