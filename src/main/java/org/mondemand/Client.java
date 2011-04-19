/*======================================================================*
 * Copyright (c) 2008, Yahoo! Inc. All rights reserved.                 *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License.  Unless required    *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/

package org.mondemand;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Enumeration;
import java.util.Vector;
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
   * CONSTANTS                    *
   ********************************/
  private final static String FILE_LINE_DELIMITER = ":";
  private final static int CALLER_DEPTH = 3;
  private final static int MAX_MESSAGES = 10;
  private static final String TRACE_KEY    = "mondemand.trace_id";
  private static final String OWNER_KEY    = "mondemand.owner";
  private static final String MESSAGE_KEY  = "mondemand.message";

  /********************************
   * CLASS ATTRIBUTES             *
   ********************************/
  private ErrorHandler errorHandler = new DefaultErrorHandler();
  private String programId = null;
  private int immediateSendLevel = Level.CRIT;
  private int noSendLevel = Level.ALL;
  private ConcurrentHashMap<String,Context> contexts = null;
  private ConcurrentHashMap<String,LogMessage> messages = null;
  private ConcurrentHashMap<String,StatsMessage> stats = null;
  private Vector<Transport> transports = null;

  /********************************
   * CONSTRUCTORS AND DESTRUCTORS *   
   ********************************/

  /**
   * The constructor creates a Client object that is ready to use.
   * @param programId a string identifying the program that is calling MonDemand
   */
  public Client (String programId) {
    /* set the default error handler */
    this.errorHandler = new DefaultErrorHandler();

    /* if the program ID is not specified, try to get it from the stack */
    if(programId == null) {
      this.programId = ClassUtils.getMainClass();
    } else {
      this.programId = programId;
    }

    /* setup internal data structures */
    contexts = new ConcurrentHashMap<String,Context>();
    messages = new ConcurrentHashMap<String,LogMessage>();
    stats = new ConcurrentHashMap<String,StatsMessage>();
    transports = new Vector<Transport>();
  }

  /**
   * Called when the client is destroyed.  Ensures that everything is cleaned up properly.
   */
  public void finalize() {
    // try to flush all logs and stats
    flushLogs();
    flushStats();

    // clear all the data
    contexts.clear();
    messages.clear();
    stats.clear();

    // shutdown all the transports
    if(transports != null) {
      for(int i=0; i<transports.size(); ++i) {
        try {
          transports.elementAt(i).shutdown();
        } catch(TransportException e) {}
      }
    }
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
    Context ctxt = new Context();
    ctxt.setKey(key);
    ctxt.setValue(value);

    if(contexts == null) {
      contexts = new ConcurrentHashMap<String,Context>();
    }
    if(key != null && value != null) {
      contexts.put(key, ctxt);
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
      Context ctxt = (Context) contexts.get(key);
      if(ctxt != null) {
        retval = ctxt.getValue();
      }
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
      contexts = new ConcurrentHashMap<String,Context>();
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
   * Adds a new transport to this client.
   * @param transport the transport object to add
   */
  public synchronized void addTransport(Transport transport) {
    if(this.transports == null) {
      this.transports = new Vector<Transport>();
    }

    if(transport != null) {
      this.transports.add(transport);
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
    dispatchLogs();
    if(messages != null) {
      messages.clear();
    }
  }

  /**
   * Flushes statistics to the transports.
   */
  public void flushStats() {
    this.flushStats(false);
  }

  /**
   * Flushes statistics to the transports, but allows one to specify whether or not to reset the 
   * running statistics.
   */
  public void flushStats(boolean reset) {
    dispatchStats();
    if(reset && stats != null) {
      stats.clear();
    }
  }

  public void flush() {
    flushLogs();
    flushStats();
  }

  /**
   * Increments the default counter by one.
   */
  public void increment() {
    this.increment(StatType.Counter, null, 1);
  }

  /**
   * Increments the default counter by value
   * @param value the amount to increment the counter by
   */
  public void increment(int value) {
    this.increment(StatType.Counter, null, value);
  }

  /**
   * Increments the specified counter by one.
   * @param key the name of the counter to increment
   */
  public void increment(String key) {
    this.increment(StatType.Counter, key, 1);
  }

  /**
   * Increments the specified counter by the value specified.
   * @param key the name of the counter to increment
   * @param value the amount to increment the counter by
   */
  public void increment(String key, int value) {
    this.increment (StatType.Counter, key, value);
  }

  public void increment (StatType type, String key, int value) {
    String realKey = key;
    StatsMessage realValue = new StatsMessage();

    // create the HashMap if it doesn't exist
    if(this.stats == null) {
      this.stats = new ConcurrentHashMap<String,StatsMessage>();
    }

    // set the key
    if(realKey == null) {
      // determine the key from the calling class and line number
      realKey = ClassUtils.getCallingClass(CALLER_DEPTH);
    }
    realValue.setKey(realKey);
    realValue.setType(type);

    // increment the counter if it exists
    StatsMessage tmp = (StatsMessage) this.stats.get(realKey);
    if(tmp != null) {
      realValue.setCounter(tmp.getCounter() + value);
    } else {
      realValue.setCounter(value);
    }

    this.stats.put(realKey, realValue);
  }

  /**
   * Decrements the default counter by one.
   */
  public void decrement() {
    this.decrement(StatType.Counter, null, 1);
  }

  /**
   * Decrements the default counter by value
   * @param value the amount to decrement the counter by
   */
  public void decrement(int value) {
    this.decrement(StatType.Counter, null, value);
  }

  /**
   * Decrements the specified counter by one.
   * @param key the name of the counter to decrement
   */
  public void decrement(String key) {
    this.increment(StatType.Counter, key, 1);
  }

  /**
   * Decrements the specified counter by the value specified.
   * @param key the name of the counter to decrement
   * @param value the amount to decrement the counter by
   */
  public void decrement(String key, int value) {
    this.increment(StatType.Counter, key, value * (-1));
  }

  public void decrement(StatType type, String key, int value) {
    this.increment(type, key, value * (-1));
  }

  /**
   * Sets the counter to the specified val ue.
   * @param key the name of the counter key to set
   * @param value the value to set this counter to
   */
  public void setKey(String key, int value) {
    this.setKey(StatType.Gauge, key, (long) value);
  }

  /**
   * Sets the counter to the specified val ue.
   * @param key the name of the counter key to set
   * @param value the value to set this counter to
   */
  public void setKey(String key, long value) {
    this.setKey(StatType.Gauge, key, (long) value);
  }

  public void setKey(StatType type, String key, long value) {
    String realKey = key;

    if(realKey == null) {
      // determine the key from the calling class and line number
      realKey = ClassUtils.getCallingClass(CALLER_DEPTH);
    }

    // create the HashMap if it doesn't exist
    if(this.stats == null) {
      this.stats = new ConcurrentHashMap<String,StatsMessage>();
    }

    // set the counter
    StatsMessage realValue = new StatsMessage();
    realValue.setKey(realKey);
    realValue.setCounter(value);
    realValue.setType(type);
    this.stats.put(realKey, realValue);
  }

  /**
   * Logs a message at priority level EMERG
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void emerg(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.EMERG, traceId, message, args);
  }

  /**
   * Logs a message a priority level EMERG, determining the calling class and line number
   * on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void emerg(TraceId traceId, String message, Object[] args) {
    emerg(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message a log level EMERG.
   * @param message
   */
  public void emerg(String message) {
    emerg(null, message, null);
  }

  /**
   * Logs a message a priority level ALERT.
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void alert(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.ALERT, traceId, message, args);
  }

  /**
   * Logs a message a priority level ALERT, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void alert(TraceId traceId, String message, Object[] args) {
    alert(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level ALERT
   * @param message the log message
   */
  public void alert(String message) {
    alert(null, message, null);
  }

  /**
   * Logs a message at priority level CRIT
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void crit(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.CRIT, traceId, message, args);
  }

  /**
   * Logs a message a priority level CRIT, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void crit(TraceId traceId, String message, Object[] args) {
    crit(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level CRIT
   * @param message the log message
   */
  public void crit(String message) {
    crit(null, message, null);
  }

  /**
   * Logs a message at priority level ERROR
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void error(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.ERROR, traceId, message, args);
  }

  /**
   * Logs a message a priority level ERROR, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void error(TraceId traceId, String message, Object[] args) {
    error(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level ERROR
   * @param message the log message
   */
  public void error(String message) {
    error(null, message, null);
  }

  /**
   * Logs a message at priority level WARNING
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void warning(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.WARNING, traceId, message, args);
  }

  /**
   * Logs a message a priority level WARNING, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void warning(TraceId traceId, String message, Object[] args) {
    warning(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level WARNING
   * @param message the log message
   */
  public void warning(String message) {
    warning(null, message, null);
  }

  /**
   * Logs a message at priority level NOTICE
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void notice(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.NOTICE, traceId, message, args);
  }

  /**
   * Logs a message a priority level NOTICE, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void notice(TraceId traceId, String message, Object[] args) {
    notice(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level NOTICE
   * @param message the log message
   */
  public void notice(String message) {
    notice(null, message, null);
  }

  /**
   * Logs a message at priority level INFO
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void info(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.INFO, traceId, message, args);
  }

  /**
   * Logs a message a priority level INFO, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void info(TraceId traceId, String message, Object[] args) {
    info(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level INFO
   * @param message the log message
   */
  public void info(String message) {
    info(null, message, null);
  }

  /**
   * Logs a message at priority level DEBUG
   * @param name the name of the calling class or filename
   * @param line the line number of the calling class or filename
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void debug(String name, int line, TraceId traceId, String message, Object[] args) {
    log(name, line, Level.DEBUG, traceId, message, args);
  }

  /**
   * Logs a message a priority level DEBUG, determining the calling class and
   * line number on the fly.
   * @param traceId an optional trace ID
   * @param message the log message
   * @param args optional arguments
   */
  public void debug(TraceId traceId, String message, Object[] args) {
    debug(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        traceId, message, args);
  }

  /**
   * A convenience method to log a message at priority level DEBUG
   * @param message the log message
   */
  public void debug(String message) {
    debug(null, message, null);
  }

  /**
   * Generic logger function.  This method will perform slower than most because it needs to detect
   * the calling class and calling line number.
   * @param level the log level of this message
   * @param traceId an optional traceId
   * @param message the log message
   * @param args optional arguments
   */
  public void log(int level, TraceId traceId, String message, Object[] args) {
    log(ClassUtils.getCallingClass(CALLER_DEPTH), ClassUtils.getCallingLine(CALLER_DEPTH),
        level, traceId, message, args);
  }

  /**
   * The most generic logger function.
   * @param name the name of this message, usually the filename or calling class
   * @param line the line number calling this message, or other numeric description of the calling class
   * @param level the log level
   * @param traceId an optional traceId
   * @param message the message
   * @param args optional arguments
   */
  public void log(String name, int line, int level,
      TraceId traceId, String message, Object[] args) {
    logReal(name, line, level, traceId, message, args);
  }

  public boolean traceMessage (String message,
                            Map<String, String> context) {
    if (context.containsKey (TRACE_KEY)
        && context.containsKey (OWNER_KEY))
      {
        return traceMessage (context.get (OWNER_KEY),
                             context.get (TRACE_KEY),
                             message,
                             context);
      }
    else
      {
        return false;
      }
  }

  public boolean traceMessage (String owner, String traceId, String message,
                               Map<String, String> context) {
    boolean ret = false;
    try {
      /* FIXME: I'm sure there's a better way to do this but I am not the best
       * java programmer.
       */
      ConcurrentHashMap<String,Context> tmp =
        new ConcurrentHashMap<String,Context>();
      for (Entry<String,String> entry : context.entrySet())
        {
          tmp.put(entry.getKey(),
                  new Context(entry.getKey(), entry.getValue()));
        }
      /* override anything set in the context with the arguments */
      tmp.put(owner, new Context(OWNER_KEY, owner));
      tmp.put(traceId, new Context(TRACE_KEY, traceId));
      tmp.put(message, new Context(MESSAGE_KEY, message));

      Context[] contexts = tmp.values().toArray(new Context[0]);

      for(int i=0; i<this.transports.size(); ++i) {
        Transport t = transports.elementAt(i);
        try {
          t.sendTrace(programId, contexts);
        } catch(TransportException te) {
          errorHandler.handleError("Error calling Transport.sendTrace()", te);
        }
      }
      ret = true;
    } catch(Exception e) {
      errorHandler.handleError("Error calling Client.traceMessage()", e);
    }

    return ret;
  }

  /********************************
   * PRIVATE API METHODS          *   
   ********************************/

  private void logReal(String name, int line, int level,
                       TraceId traceId, String message, Object[] args)
  {
    String filename = name;
    StringBuffer formattedMsg = new StringBuffer();

    if(message == null) return;
    if(level < Level.OFF || level > Level.ALL) {
      errorHandler.handleError("Client.logReal() called by " + 
          name + ":" + line + " with invalid log level: " + Integer.toString(level));
    }

    // initialize if necessary
    if(this.messages == null) {
      this.messages = new ConcurrentHashMap<String, LogMessage>();
    }

    // format the message
    formattedMsg.append(message);
    if(args != null) {
      for(int i=0; i<args.length; ++i) {
        formattedMsg.append(" " + args[i].toString());
      }
    }

    // figure out the name if it is null
    if(filename == null) {
      filename = ClassUtils.getCallingClass(CALLER_DEPTH);
    }

    try {
      if(levelIsEnabled(level, traceId)) {
        String key = filename + FILE_LINE_DELIMITER + Integer.toString(line);
        if(this.messages.containsKey(key)) {
          // repeated message, increment the repeat counter
          LogMessage msg = (LogMessage) this.messages.get(key);
          if(msg != null) {
            msg.setRepeat(msg.getRepeat() + 1);
            if(msg.getRepeat() % 999 == 0) {
              flushLogs();
              return;
            }

            this.messages.put(key, msg);
          }
        } else {
          // new message
          LogMessage msg = new LogMessage();
          msg.setFilename(filename);
          msg.setLine(line);
          msg.setLevel(level);
          msg.setMessage(formattedMsg.toString());
          msg.setRepeat(1);
          msg.setTraceId(traceId);
          this.messages.put(key, msg);
        }

        // if the trace ID is set, emit immediately
        if(traceId != null) {
          if(traceId.compareTo(TraceId.NULL_TRACE_ID) != 0) {
            flushLogs();
            return;
          }
        }

        // if the immediate send level is passed, emit immediately
        if(level <= this.immediateSendLevel) {
          flushLogs();
          return;
        }

        // if the message buffer is full, emit immediately
        if(messages.size() >= MAX_MESSAGES) {
          flushLogs();
          return;
        }
      }
    } catch(Exception e) {
      errorHandler.handleError("Error in Client.logReal()", e);
    }
  }

  /**
   * Iterates through the transports, calling the sendLogs method for each.
   * Since we cannot assume transports are thread-safe, we make this method synchronized.
   */
  private synchronized void dispatchLogs() {
    if(this.messages == null || this.transports == null) return;

    try {
      Context[] contexts = this.contexts.values().toArray(new Context[0]);
      LogMessage[] messages = this.messages.values().toArray(new LogMessage[0]);

      for(int i=0; i<this.transports.size(); ++i) {
        Transport t = transports.elementAt(i);
        try {
          t.sendLogs(programId, messages, contexts);
        } catch(TransportException te) {
          errorHandler.handleError("Error calling Transport.sendLogs()", te);
        }
      }
    } catch(Exception e) {
      errorHandler.handleError("Error calling Client.dispatchLogs()", e);
    }
  }

  /**
   * Iterates through the transports, calling the sendStats method for each.
   * Since we cannot assume transports are thread-safe, we make this method synchronized.
   */
  private synchronized void dispatchStats() {
    if(this.stats == null || this.transports == null) return;

    try {
      Context[] contexts = this.contexts.values().toArray(new Context[0]);
      StatsMessage[] messages = this.stats.values().toArray(new StatsMessage[0]);

      for(int i=0; i<this.transports.size(); ++i) {
        Transport t = transports.elementAt(i);
        try {
          t.sendStats(programId, messages, contexts);
        } catch(TransportException te) {
          errorHandler.handleError("Error calling Transport.sendStats()", te);
        }
      }
    } catch(Exception e) {
      errorHandler.handleError("Error calling Client.dispatchStats()", e);
    }
  }
}
