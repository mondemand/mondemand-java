package org.mondemand;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import org.mondemand.util.ClassUtils;

/**
 * This is the main entry point to MonDemand.  Users can create client objects and use them to
 * log messages and statistics.
 * @author Michael Lum
 *
 */
public class Client {
	
	/********************************
	 * CLASS ATTRIBUTES             *   
	 ********************************/
	private ErrorHandler errorHandler = new DefaultErrorHandler();
	private String programId = null;
	private int immediateSendLevel = Level.CRIT;
	private int noSendLevel = Level.ALL;
	private ConcurrentHashMap<String,String> contexts = null;
	private ConcurrentHashMap<String,Long> stats = null;
	
	/********************************
	 * CONSTRUCTORS AND DESTRUCTORS *   
	 ********************************/
	
	/**
	 * The constructor creates a Client object that is ready to use.
	 * @param programId a string identifying the program that is calling MonDemand
	 */
	public Client(String programId) {
		/* set the error handler if its not set */
		if(this.errorHandler == null) {
			this.errorHandler = new DefaultErrorHandler();
		}		

		/* if the program ID is not specified, try to get it from the stack */
		if(programId == null) {
			this.programId = ClassUtils.getMainClass();
		} else {
			this.programId = programId;
		}		
		
		/* setup internal data structures */
		contexts = new ConcurrentHashMap<String,String>();
		stats = new ConcurrentHashMap<String,Long>();
	}
	
	/**
	 * Called when the client is destroyed.  Ensures that everything is cleaned up properly.
	 */
	public void finalize() {
		
	}

	/********************************
	 * ACCESSORS AND MUTATORS       *   
	 ********************************/
	
	/**
	 * @return the programId
	 */
	public String getProgramId() {
		return programId;
	}

	/**
	 * @param programId the programId to set
	 */
	public void setProgramId(String programId) {
		this.programId = programId;
	}

	/**
	 * @return the immediateSendLevel
	 */
	public int getImmediateSendLevel() {
		return immediateSendLevel;
	}

	/**
	 * @param immediateSendLevel the immediateSendLevel to set
	 */
	public void setImmediateSendLevel(int immediateSendLevel) {
		this.immediateSendLevel = immediateSendLevel;
	}

	/**
	 * @return the noSendLevel
	 */
	public int getNoSendLevel() {
		return noSendLevel;
	}

	/**
	 * @param noSendLevel the noSendLevel to set
	 */
	public void setNoSendLevel(int noSendLevel) {
		this.noSendLevel = noSendLevel;
	}

	/**
	 * @return the errorHandler
	 */
	public ErrorHandler getErrorHandler() {
		return errorHandler;
	}

	/**
	 * Sets a custom error handler.  Cowardly refuses to set it to null.
	 * @param errorHandler the errorHandler to set
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		if(errorHandler != null) {
			this.errorHandler = errorHandler;
		}
	}
	
	/********************************
	 * PUBLIC API METHODS           *   
	 ********************************/
	
	/**
	 * Adds contextual data to the client.
	 */
	public void addContext(String key, String value) {
		if(contexts == null) {
			contexts = new ConcurrentHashMap<String,String>();
		}
		if(key != null && value != null) {
			contexts.put(key, value);
		}
	}
	
	/**
	 * Removes contextual data from the client.
	 */
	public void removeContext(String key) {
		if(contexts != null && key != null) {
			contexts.remove(key);
		}
	}
	
	/**
	 * Fetches contextual data from the client.
	 */
	public String getContext(String key) {
		String retval = null;
		
		if(contexts != null && key != null) {
			retval = (String) contexts.get(key);
		}
		
		return retval;
	}
	
    /**
     * Retrieves an enumeration of all the contextual data keys
     * 
     * @return an enumeration of all keys
     */
    public Enumeration<String> getContextKeys() {
            if (contexts == null) {
            	contexts = new ConcurrentHashMap<String,String>();
            }
            return contexts.keys();
    }

    /**
     * Clear contextual data from the logger. Contextual data persists between
     * flush() calls and is only removed if you call removeAllContexts().
     */
    public void removeAllContexts() {
    	if(contexts != null) {
            contexts.clear();
    	}
    }

    /**
     * A check for the log level that is set.
     * 
     * @param level the priority level to check
     * @param traceId the TraceId to check for
     * @return true if this level is enabled, false otherwise
     */
    public boolean levelIsEnabled(int level, TraceId traceId) {
    	if (traceId == null) {
    		return level < this.noSendLevel;
    	} else {
            return ((traceId.compareTo(TraceId.NULL_TRACE_ID) != 0) || 
            		(level < this.noSendLevel));
    	}
    }

    /**
     * Flushes log data to the transports.
     */
    public void flushLogs() {
    	
    }
    
    /**
     * Flushes statistics to the transports.
     */
    public void flushStats() {
    	this.flushStats(true);
    }
    
    /**
     * Flushes statistics to the transports, but allows one to specify whether or not to reset the 
     * running statistics.
     */
    public void flushStats(boolean reset) {
    }
	
    /**
     * Increments the default counter by one.
     */
    public void increment() {
    	this.increment(null, 1);
    }
    
    /**
     * Increments the default counter by value
     * @param value the amount to increment the counter by
     */
    public void increment(int value) {
    	this.increment(null, value);
    }
    
    /**
     * Increments the specified counter by one.
     * @param key the name of the counter to increment
     */
    public void increment(String key) {
    	this.increment(key, 1);
    }
    
    /**
     * Increments the specified counter by the value specified.
     * @param key the name of the counter to increment
     * @param value the amount to increment the counter by
     */
    public void increment(String key, int value) {
    	String realKey = key;
    	long realValue = value;
    	
    	if(realKey == null) {
    		// determine the key from the calling class and line number
    		realKey = ClassUtils.getCallingClass(3);
    	}
    	
    	// create the HashMap if it doesn't exist
    	if(this.stats == null) {
    		this.stats = new ConcurrentHashMap<String,Long>();
    	}
    	
    	// increment the counter if it exists
    	Long tmp = (Long) this.stats.get(realKey);
    	if(tmp != null) {
    		realValue = tmp.longValue() + value;
    	}
    	
    	this.stats.put(realKey, realValue);
    }
    
    /**
     * Decrements the default counter by one.
     */
    public void decrement() {
    	this.decrement(null, 1);
    }
    
    /**
     * Decrements the default counter by value
     * @param value the amount to decrement the counter by
     */
    public void decrement(int value) {
    	this.decrement(null, value);
    }
    
    /**
     * Decrements the specified counter by one.
     * @param key the name of the counter to decrement
     */
    public void decrement(String key) {
    	this.increment(key, 1);
    }
    
    /**
     * Decrements the specified counter by the value specified.
     * @param key the name of the counter to decrement
     * @param value the amount to decrement the counter by
     */
    public void decrement(String key, int value) {
    	this.increment(key, value * (-1));
    }

    /**
     * Sets the counter to the specified val ue.
     * @param key the name of the counter key to set
     * @param value the value to set this counter to
     */
    public void setKey(String key, int value) {
    	setKey(key, value);
    }
    
    /**
     * Sets the counter to the specified val ue.
     * @param key the name of the counter key to set
     * @param value the value to set this counter to
     */
    public void setKey(String key, long value) {
    	String realKey = key;
    	
    	if(realKey == null) {
    		// determine the key from the calling class and line number
    		realKey = ClassUtils.getCallingClass(3);
    	}
    	
    	// create the HashMap if it doesn't exist
    	if(this.stats == null) {
    		this.stats = new ConcurrentHashMap<String,Long>();
    	}
    	
    	// set the counter
    	Long realValue = new Long(value);    	
    	this.stats.put(realKey, realValue);    	
    }
    
    
	/********************************
	 * PRIVATE API METHODS          *   
	 ********************************/
}
