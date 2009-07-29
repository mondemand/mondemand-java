package org.mondemand;

import java.io.Serializable;

public class StatsMessage implements Serializable {
	private String key = null;
	private long counter = 0;
	
	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}
	
	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}
	
	/**
	 * @return the counter
	 */
	public long getCounter() {
		return counter;
	}
	
	/**
	 * @param counter the counter to set
	 */
	public void setCounter(long counter) {
		this.counter = counter;
	}
	
	public String toString() {
		return key + ": counter=" + counter;
	}
}
