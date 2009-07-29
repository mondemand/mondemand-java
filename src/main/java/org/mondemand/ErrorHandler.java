package org.mondemand;

/**
 * This interface provides a way to create error handler callbacks.
 * @author Michael Lum
 */
public interface ErrorHandler {
	public void handleError(String error);
	public void handleError(String error, Exception e);     
}

