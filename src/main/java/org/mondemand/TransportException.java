package org.mondemand;

/**
 * Class for throwing exceptions from transport implementations
 * @author Michael Lum
 *
 */
public class TransportException extends Exception {

	public TransportException() {
		super();
	}
	
	public TransportException(String message) {
		super(message);
	}
	
	public TransportException(Exception e) {
		super(e);
	}
	
	public TransportException(String message, Exception e) {
		super(message, e);
	}
}
