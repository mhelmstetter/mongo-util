package com.mongodb.util;

public class ThreadUtils {
	
	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}
	
	

}
