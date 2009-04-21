package org.mondemand;

public class TraceId implements Comparable<TraceId> {
	  public final static TraceId NULL_TRACE_ID = new TraceId(0);	
		
	  /* the internal ID */
	  private long id;

	  /**
	   * Create a trace ID with the given identifier
	   * @param id the identifier to use
	   */
	  public TraceId(long id) {
		  if ((new Long(id)) != null) {
			  this.id = id;
		  }
	  }
	  
	  /**
	   * Default constructor, if no id is given
	   */
	  public TraceId() {
		  this.id = 0;
	  }

	  /**
	   * Accessor which returns the identifier for this trace id
	   * 
	   * @return the id as a long
	   */
	  public long getId() {
		  return this.id;
	  }

	  /**
	   * Mutator which allows the identifier to be overwritten
	   * 
	   * @param newId the new identifier for this trace id
	   */
	  public void setId(long newId) {
		  this.id = newId;
	  }

	  /**
	   * Compares this TraceId with another.
	   *
	   * @return 1 if the caller's id is greater than the arg's id, -1 if
	   * it is less than, and 0 if they are equal.
	   */
	  public int compareTo(TraceId traceId) {
		  long t1 = traceId.getId();
	    
		  if (this.id < t1) {
			  return -1;
		  } else if (this.id > t1) {
			  return 1;
		  } 

		  return 0;
	  }
}
