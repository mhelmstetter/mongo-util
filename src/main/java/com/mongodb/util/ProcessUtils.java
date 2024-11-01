package com.mongodb.util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUtils {
	
	private static Logger logger = LoggerFactory.getLogger(ProcessUtils.class);

    /**
     * Finds all instances of a given binary that are currently running.
     *
     * @param processName The name of the process to look for.
     * @return A list of ProcessInfo objects for each matching process found.
     */
    public static List<ProcessInfo> getRunningProcesses(String processName) {
        return ProcessHandle.allProcesses()
                .filter(process -> process.info().command().map(cmd -> cmd.endsWith(processName)).orElse(false))
                .map(ProcessUtils::toProcessInfo)
                .collect(Collectors.toList());
    }

    /**
     * Finds and kills all instances of a given binary and returns their details.
     *
     * @param processName The name of the process to kill.
     * @return A list of ProcessInfo objects for each matching process that was killed.
     */
    public static List<ProcessInfo> killAllProcesses(String processName) {
        List<ProcessInfo> killedProcesses = new ArrayList<>();
        final AtomicInteger killAttempts = new AtomicInteger(5);
        ProcessHandle.allProcesses()
                .filter(process -> process.info().command().map(cmd -> cmd.endsWith(processName)).orElse(false))
                .forEach(process -> {
                    if (process.isAlive()) {
                        boolean killed = process.destroy();  // Attempt graceful termination
                        
                        while (!killed && killAttempts.get() <= 5) {
                        	try {
								Thread.sleep(1000 * killAttempts.get());
							} catch (InterruptedException e) {
							}
                        	killAttempts.incrementAndGet();
                        }
                        
                        killAttempts.set(0);
                        if (!killed && process.isAlive()) {  // If still alive, force kill
                            killed = process.destroyForcibly();
                        }
                        
                        while (!killed && killAttempts.get() <= 5) {
                        	try {
								Thread.sleep(1000 * killAttempts.get());
							} catch (InterruptedException e) {
							}
                        	killAttempts.incrementAndGet();
                        }

                        if (killed && !process.isAlive()) {  // Confirm the process is no longer alive
                            killedProcesses.add(toProcessInfo(process));
                        } else {
                            logger.error("Failed to kill process with PID: {}", process.pid());
                        }
                    }
                });

        return killedProcesses;
    }

    /**
     * Helper method to convert a ProcessHandle to a ProcessInfo object.
     *
     * @param process The ProcessHandle to convert.
     * @return A ProcessInfo object with details of the process.
     */
    private static ProcessInfo toProcessInfo(ProcessHandle process) {
        ProcessHandle.Info info = process.info();
        long pid = process.pid();
        String command = info.command().orElse("unknown");
        List<String> arguments = info.arguments().map(List::of).orElse(List.of());
        Instant startTime = info.startInstant().orElse(null);
        String user = info.user().orElse("unknown");

        return new ProcessInfo(pid, command, arguments, startTime, user);
    }
    
    public static class ProcessInfo {
        private final long pid;
        private final String command;
        private final List<String> arguments;
        private final Instant startTime;
        private final String user;

        public ProcessInfo(long pid, String command, List<String> arguments, Instant startTime, String user) {
            this.pid = pid;
            this.command = command;
            this.arguments = arguments;
            this.startTime = startTime;
            this.user = user;
        }

        public long getPid() {
            return pid;
        }

        public String getCommand() {
            return command;
        }

        public List<String> getArguments() {
            return arguments;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "ProcessInfo{" +
                    "pid=" + pid +
                    ", command='" + command + '\'' +
                    ", arguments=" + arguments +
                    ", startTime=" + startTime +
                    ", user='" + user + '\'' +
                    '}';
        }
    }

}