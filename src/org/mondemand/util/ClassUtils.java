package org.mondemand.util;

/**
 * Various class utilities that are used when the user doesn't specify parameters in the API.
 * These methods do not usually perform very well due to the need to obtain stack traces, so it is almost always
 * preferred for the user to specify these parameters in the MonDemand API instead of trying to determine
 * them using this class at runtime.
 * 
 * @author Michael Lum
 */
public class ClassUtils {
	/**
	 * Returns the top-level class name.   Useful when trying to determine the name of the application
	 * calling the system when it is not specified.
	 * @return the name of the top-most class in the calling stack
	 */
	public static String getMainClass()
	{
		StackTraceElement[] stack = Thread.currentThread().getStackTrace();
		StackTraceElement main = stack[stack.length - 1];
		return main.getClassName();
	}
	
	/**
	 * Returns the calling class from the depth specified.
	 * @param depth the depth to traverse in the stack
	 * @return the name of the calling class in the stack
	 */
    public static String getCallingClass(int depth) {
    	try {
    		StackTraceElement[] st = (new Throwable()).getStackTrace();
            if (st != null) {
            	if(depth > st.length) {
            		return st[st.length-1].getClassName();
            	} else {
            		return st[depth].getClassName();
            	}            	
            }
        } catch (Exception e) {
        	// do nothing, we'll just return the name of this class
        }

        return ClassUtils.getMainClass();
    }
    
    /**
     * Returns the calling line number from the depth specified.
     * @param depth the depth to traverse in the stack
     * @return the line number of the calling class in the stack
     */
    public static int getCallingLine(int depth) {
        try {
        	StackTraceElement[] st = (new Throwable()).getStackTrace();
            if (st != null) {
            	if(depth > st.length) {
            		return st[st.length-1].getLineNumber();
            	} else {
            		return st[depth].getLineNumber();
            	}
            }
        } catch (Exception e) {
        	// do nothing and return zero
        }

        return 0;
    }
}
