package org.mondemand;

import java.io.Serializable;

public class LogMessage implements Serializable {
	private String filename = null;
	private int line = 0;
	private int level = Level.DEBUG;
	private String message = null;
	private int repeat = 0;
	private TraceId traceId = null;
	
	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}
	
	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	/**
	 * @return the line
	 */
	public int getLine() {
		return line;
	}
	
	/**
	 * @param line the line to set
	 */
	public void setLine(int line) {
		this.line = line;
	}
	
	/**
	 * @return the level
	 */
	public int getLevel() {
		return level;
	}
	
	/**
	 * @param level the level to set
	 */
	public void setLevel(int level) {
		this.level = level;
	}
	
	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}
	
	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}
	
	/**
	 * @return the repeat
	 */
	public int getRepeat() {
		return repeat;
	}
	
	/**
	 * @param repeat the repeat to set
	 */
	public void setRepeat(int repeat) {
		this.repeat = repeat;
	}
	
	/**
	 * @return the traceId
	 */
	public TraceId getTraceId() {
		return traceId;
	}
	
	/**
	 * @param traceId the traceId to set
	 */
	public void setTraceId(TraceId traceId) {
		this.traceId = traceId;
	}
	
}
