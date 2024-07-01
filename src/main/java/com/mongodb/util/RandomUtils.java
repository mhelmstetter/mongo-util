package com.mongodb.util;

import java.util.Random;
import java.util.Set;

public class RandomUtils {
	
	private final static Random random = new Random();
	
	public static <T> T getRandomElementFromSet(Set<T> set) {
	    if (set == null || set.isEmpty()) {
	        throw new IllegalArgumentException("The Set cannot be empty.");
	    }
	    int randomIndex = random.nextInt(set.size());
	    int i = 0;
	    for (T element : set) {
	        if (i == randomIndex) {
	            return element;
	        }
	        i++;
	    }
	    throw new IllegalStateException("Something went wrong while picking a random element.");
	}

}
