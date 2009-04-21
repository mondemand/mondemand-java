package org.mondemand;

import java.io.Serializable;

/**
 * This represents the various logging levels in MonDemand
 * @author Michael Lum
 */
public class Level implements Serializable {

	/**
	 * textual representation
	 */
	public final static String STRINGS[] = { "emerg", "alert", "crit",
		"error", "warning", "notice", "info", "debug", "all" };	
	
	/**
	 * all off
	 */
	public final static short OFF = -1;
	
	/**
	 * Default emergency priority value for the Logger
	 */
	public final static short EMERG = 0;

	/**
	 * Default alert priority value for the Logger
	 */
	public final static short ALERT = 1;

	/**
	 * Default critical priority value for the Logger
	 */
	public final static short CRIT = 2;

	/**
	 * Default error priority value for the Logger
	 */
	public final static short ERROR = 3;

	/**
	 * Default warning priority value for the Logger
	 */
	public final static short WARNING = 4;

	/**
	 * Default notice priority value for the Logger
	 */
	public final static short NOTICE = 5;

	/**
	 * Default info priority value for the Logger
	 */
	public final static short INFO = 6;

	/**
	 * Default debug priority value for the Logger
	 */
	public final static short DEBUG = 7;

	/**
	 * Default value to send all messages for the Logger
	 */
	public final static short ALL = 8;	
	
}
