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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.mondemand.transport.LWESTransport;
import org.mondemand.util.ClassUtils;

import com.google.common.util.concurrent.AtomicLongMap;

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
  private static final String CONFIG_FILE  = "/etc/mondemand/mondemand.conf";
  private static final int    EMIT_INTERVAL = 60;   // 60 seconds
  private static final boolean DEFAULT_AUTO_EMIT = false;   // auto emit disabled by default
  private static final boolean DEFAULT_CLEAR_STAT = false;  // clear stats after flush by auto emit

  /********************************
   * CLASS ATTRIBUTES             *
   ********************************/
  private ErrorHandler errorHandler = new DefaultErrorHandler();
  private String programId = null;
  private int immediateSendLevel = Level.CRIT;
  private int noSendLevel = Level.ALL;
  private final ConcurrentHashMap<String,Context> contexts;
  private volatile ConcurrentHashMap<String,LogMessage> messages;
  private volatile ConcurrentHashMap<String,StatsMessage> stats;
  private volatile ConcurrentHashMap<String,SamplesMessage> samples;
  private volatile ConcurrentHashMap<ContextList, AtomicLongMap<String>> contextStats;
  private final ConcurrentHashMap<EventType, List<Transport>> transports;
  private final ClientStatEmitter autoStatEmitter;
  private final Thread emitterThread;
  private Integer maxNumMetrics = null;

  /********************************
   * CONSTRUCTORS AND DESTRUCTORS *
   ********************************/

  /**
   * object to auto emit the stats and logs
   */
  public class ClientStatEmitter implements Runnable {
    private final Client client;            // client to emit stats for
    private final int intervalMS;           // emit interval in milli seconds
    private final boolean clearStats;       // if the stats should be cleared after flush
    private volatile boolean stop = false;  // if the emitter should stop
    /**
     * constructor
     * @param client - the client to emit stats for
     * @param interval - emission interval in seconds
     * @param clearStats - whether or not stats should be cleared after emit
     */
    ClientStatEmitter(Client client, int interval, boolean clearStats) {
      this.client = client;
      this.intervalMS = interval * 1000;
      this.clearStats = clearStats;
    }

    /**
     * make the auto-emit thread stop
     */
    public void stop() {
      stop = true;
    }

    /**
     * the run method for the emitter thread. it would sleep for interval and
     * then emit the stats, and keep doing the same until it is interrupted to
     * stop
     */
    @Override
    public void run() {
      while(!stop) {
        try {
          Thread.sleep(intervalMS);
          client.flush(clearStats);
        } catch (InterruptedException e) {
          // if we are interrupted, it will check for the stop flag
        }
      }
      // final flush
      client.flush(clearStats);
    }
  }

  /**
   * The constructor creates a Client object that is ready to use.
   *
   * @param programId a string identifying the program that is calling MonDemand
   * @param autoStatEmit specifies if auto stat emit is enabled
   * @param clearStatAfterEmit specifies if stats should be cleared after each auto emit
   * @param statEmitInterval - auto stat emit interval, ignored if auto
   *        emission is disabled
   */
  public Client(String programId, boolean autoStatEmit, boolean clearStatAfterEmit,
      int statEmitInterval) {
    /* set the default error handler */
    this.errorHandler = new DefaultErrorHandler();

    /* if the program ID is not specified, try to get it from the stack */
    if(programId == null) {
      this.programId = ClassUtils.getMainClass();
    } else {
      this.programId = programId;
    }

    /* setup internal data structures */
    contexts = new ConcurrentHashMap<>();
    messages = new ConcurrentHashMap<>();
    stats = new ConcurrentHashMap<>();
    samples = new ConcurrentHashMap<>();
    transports = new ConcurrentHashMap<>();
    contextStats = new ConcurrentHashMap<>();

    // initialize transports with empty lists
    for (EventType eventType : EventType.values()) {
      transports.put(eventType, new CopyOnWriteArrayList<Transport>());
    }

    // create and start the emitter thread
    if(autoStatEmit) {
      // make sure interval is greater than 1, otherwise use the default
      statEmitInterval = statEmitInterval <= 0 ? EMIT_INTERVAL : statEmitInterval;
      autoStatEmitter = new ClientStatEmitter(this, statEmitInterval, clearStatAfterEmit);
      emitterThread = new Thread( autoStatEmitter );
      emitterThread.start();
    }
    else {
      autoStatEmitter = null;
      emitterThread = null;
    }
  }

  /**
   * The constructor creates a Client object that is ready to use, uses default
   * interval of 60 seconds for auto-emit if autoStatEmit is set.
   *
   * @param programId a string identifying the program that is calling MonDemand
   * @param autoStatEmit specifies if auto stat emit is enabled
   * @param clearStatAfterEmit specifies if stats should be cleared after each auto emit
   */
  public Client(String programId, boolean autoStatEmit, boolean clearStatAfterEmit) {
    this(programId, autoStatEmit, clearStatAfterEmit, EMIT_INTERVAL);
  }

  /**
   * The constructor creates a Client object that is ready to use, uses default
   * interval of 60 seconds for auto-emit if autoStatEmit is set.
   *
   * @param programId a string identifying the program that is calling MonDemand
   * @param autoStatEmit specifies if auto stat emit is enabled
   */
  public Client(String programId, boolean autoStatEmit) {
    this(programId, autoStatEmit, DEFAULT_CLEAR_STAT, EMIT_INTERVAL);
  }

  /**
   * The constructor creates a Client object that is ready to use. the auto-stat
   * emit for the client created with this constructor is turned off.
   *
   * @param programId a string identifying the program that is calling MonDemand
   */
  public Client (String programId) {
    this(programId, DEFAULT_AUTO_EMIT, DEFAULT_CLEAR_STAT, EMIT_INTERVAL);
  }

  /**
   * The constructor creates a Client object that is ready to use.
   * @param programId a string identifying the program that is calling Mondemand
   * @param host a string identifying the host (i.e. InetAddress.getLocalHost().getHostName()) where the program is running.
   */
  public Client (String programId, String host) {
    this(programId);
    addContext("host", host);
  }

  /**
   * Called when the client is destroyed.  Ensures that everything is cleaned up properly.
   */
  @Override
  public void finalize() {
    // try to flush all logs, stats and samples
    flush();

    // stop the auto-emit thread
    if (emitterThread != null) {
      try {
        autoStatEmitter.stop();
        emitterThread.interrupt();
        emitterThread.join();
      } catch (Exception e) {
        // ignore the exception for join
      }
    }

    // shutdown all the transports
    Set<Transport> seenTransports = new HashSet<>();

    for (List<Transport> transportList : transports.values()) {
      for (Transport t : transportList) {
        if (!seenTransports.contains(t)) {
          seenTransports.add(t);

          try {
            t.shutdown();
          } catch (TransportException e) {}
        }
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

  /**
   * @param maxNumMetrics the maximum number of metrics to send in stats messages
   */
  public void setMaxNumMetrics(Integer maxNumMetrics)
  {
    this.maxNumMetrics = maxNumMetrics;
  }

  /********************************
   * PUBLIC API METHODS           *
   ********************************/

  /**
   * Adds contextual data to the client.
   */
  public void addContext(String key, String value) {
    Context ctxt = new Context(key, value);

    if(key != null && value != null) {
      contexts.put(key, ctxt);
    }
  }

  /**
   * Removes contextual data from the client.
   */
  public void removeContext(String key) {
    if(key != null) {
      contexts.remove(key);
    }
  }

  /**
   * Fetches contextual data from the client.
   */
  public String getContext(String key) {
    String retval = null;

    if(key != null) {
      Context ctxt = contexts.get(key);
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
    return contexts.keys();
  }

  /**
   * Clear contextual data from the logger. Contextual data persists between
   * flush() calls and is only removed if you call removeAllContexts().
   */
  public void removeAllContexts() {
    contexts.clear();
  }

  /**
   * adds transports from the default configuration file
   * at "/etc/mondemand/mondemand.conf"
   * @throws FileNotFoundException if config file could not be found
   * @throws IOException if config file could not be read
   * @throws TransportException if there was an error creating the LWESTransport
   * @throws UnknownHostException if a bad host is specified
   * @throws IllegalArgumentException if default config file does not exist,
   *        or if there is a problem reading the file, or either port or
   *        address is missing, or if port cannot be converted to number, or if
   *        addresses cannot be converted to valid hosts, or if ttl cannot be
   *        converted to number in valid range, or if sendto cannot be converted
   *        to number in valid range, or if length of port or ttl arrays does
   *        not equal one or length of addr array, or if a transport cannot be
   *        created for addresses/port specified in the file.
   * @throws NumberFormatException if a bad PORT, TTL, or SENDTO is specified
   */
  public void addTransportsFromDefaultConfigFile()
      throws FileNotFoundException, IOException, TransportException,
             UnknownHostException {
    this.addTransportsFromConfigFile(CONFIG_FILE);
  }

  /**
   * adds transports from a configuration file. the format of the file is:
   * MONDEMAND_ADDR="<ip>[,<ip>]?"
   * MONDEMAND_PORT="<port>[,<port>]?"
   * (optional) MONDEMAND_TTL="<ttl>[,<ttl>]?"
   * (optional) MONDEMAND_SENDTO="<sendto>"
   *
   * @param configFileName - configuration file name.
   * @throws FileNotFoundException if config file could not be found
   * @throws IOException if config file could not be read
   * @throws TransportException if there was an error creating the LWESTransport
   * @throws UknownHostException if a bad host is specified
   * @throws IllegalArgumentException if file does not exist, or if there is a
   *        problem reading the file, or either port or address is missing, or
   *        if port cannot be converted to number, or if addresses cannot be
   *        converted to valid hosts, or if ttl cannot be converted to number in
   *        valid range, or if sendto cannot be converted to number in valid
   *        range, or if length of port or ttl arrays does not equal one or
   *        length of addr array, or if a transport cannot be created for
   *        addresses/port specified in the file.
   * @throws NumberFormatException if a bad PORT, TTL, or SENDTO is specified
   */
  public void addTransportsFromConfigFile(String configFileName)
      throws FileNotFoundException, IOException, TransportException,
             UnknownHostException {
    Properties prop = new Properties();
    InputStream input = null;

    try {
      // load a properties file
      input = new FileInputStream(configFileName);
      prop.load(input);

      // build config defaults first
      Config defaults = ConfigBuilder.buildDefaultConfig(prop);

      // build event-specific configs
      for (EventType eventType : EventType.values()) {
        EventSpecificConfig eventSpecific =
          ConfigBuilder.buildEventSpecificConfig(prop, eventType, defaults);

        Properties emitterGroupProps =
          eventSpecific.toEmitterGroupProperties(eventType);

        // add transport for each event type
        addTransport(eventType,
                     new LWESTransport(emitterGroupProps, eventType.name()));
      }
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (Exception e) {
          // ignore this exception
        }
      }
    }
  }

  /**
   * Adds a new transport to this client.
   * @param transport the transport object to add
   */
  public void addTransport(Transport transport) {
    for (EventType eventType : EventType.values()) {
      addTransport(eventType, transport);
    }
  }

  /**
   * Adds a new event-specific transport to this client.
   * @param eventType the event type for this transport
   * @param transport the transport object to add
   */
  public void addTransport(EventType eventType, Transport transport) {
    if (null == transport) {
      return;
    }

    this.transports.get(eventType).add(transport);
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
   * flushes all logs, stats, and samples to the transports.
   */
  public void flush() {
    this.flush(false);
  }

  /**
   * flushes all logs, stats, and samples to the transports.
   * @param resetStats - whether or not stats should be reset after flush
   */
  public void flush(boolean resetStats) {
    Context[] currentContext = contexts.values().toArray(new Context[0]);
    flushLogs(currentContext);

    ConcurrentHashMap<String,StatsMessage> prevStats = stats;
    ConcurrentHashMap<ContextList, AtomicLongMap<String>> prevContextStats = contextStats;
    if (resetStats) {
      stats = new ConcurrentHashMap<>(prevStats.size());
      contextStats = new ConcurrentHashMap<>(prevContextStats.size());
    }
    // Always reset samples
    ConcurrentHashMap<String,SamplesMessage> prevSamples = samples;
    samples = new ConcurrentHashMap<>(prevSamples.size());

    StatsMessage[] prevStatsArray = prevStats.values().toArray(new StatsMessage[0]);
    SamplesMessage[] prevSamplesArray = prevSamples.values().toArray(new SamplesMessage[0]);
    dispatchStatsSamples(prevStatsArray, prevSamplesArray, currentContext);
    dispatchContextStats(prevContextStats, currentContext);
  }

  /**
   * Flushes log data to the transports.
   */
  public void flushLogs() {
    Context[] currentContext = contexts.values().toArray(new Context[0]);
    flushLogs(currentContext);
  }

  /**
   * Flushes log data to the transports.
   */
  private void flushLogs(Context[] currentContext) {
    ConcurrentHashMap<String,LogMessage> prevMessages = messages;
    messages = new ConcurrentHashMap<>(prevMessages.size());
    dispatchLogs(prevMessages, currentContext);
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
    this.increment(StatType.Counter, key, value);
  }

  /**
   * increment a counter
   * @param type - type of the counter
   * @param key - the name of the counter to increment
   * @param value - the amount to increment the counter by
   */
  public void increment(StatType type, String key, int value) {
    String realKey = key;

    // set the key
    if(realKey == null) {
      // determine the key from the calling class and line number
      realKey = ClassUtils.getCallingClass(CALLER_DEPTH);
    }

    // Note: increment could be lost due to a race condition but no
    //       synchronization is required.
    StatsMessage realValue = this.stats.get(realKey);
    if(realValue == null) {
      // create the counter if doesn't exist
      StatsMessage newValue = new StatsMessage(realKey, type);
      realValue = this.stats.putIfAbsent(realKey, newValue);
      if(realValue == null) {
        realValue = newValue;
      }
    }
    // update the counter
    realValue.incrementBy(value);
  }

  /**
   * Given context&Stats map, increment according to context and key/value
   * @param context context
   * @param keyType key
   * @param value value
   */
  public void increment(ContextList context, String keyType, long value)
  {
    // Note: add could be lost due to a race condition but no
    //       synchronization is required.
    AtomicLongMap<String> stats = contextStats.get(context);
    if (stats == null)
    {
      AtomicLongMap<String> newStats = AtomicLongMap.create();
      stats = contextStats.putIfAbsent(context, newStats);
      if(stats == null) {
        stats = newStats;
      }
    }
    stats.addAndGet(keyType, value);
  }

  /**
   * Increment the count for the map
   * @param context : context
   * @param keyType : KeyType: blank, advertiser_revenue, etc
   */
  public void increment(ContextList context, String keyType)
  {
    increment(context, keyType, 1);
  }

  /**
   * adds a new sample
   * @param key - the name of the sample to add a new value to
   * @param value - the amount to be added to sample
   * @param trackingTypeValue - bitwise value, specifies what extra stats
   *        (min/max/...) should be kept for a counter
   */
  public void addSample(String key, int value, int trackingTypeValue) {
    this.addSample(key, value, trackingTypeValue, 0);
  }

  /**
   * adds a new sample
   * @param key - the name of the sample to add a new value to
   * @param value - the amount to be added to sample
   * @param trackingTypeValue - bitwise value, specifies what extra stats
   *        (min/max/...) should be kept for a counter
   * @param samplesMaxCount - maximum number of samples to keep, ignored if
   *        less than or equal to 0.
   */
  public void addSample(String key, int value, int trackingTypeValue, int samplesMaxCount) {
    String realKey = key;

    // set the key
    if(realKey == null) {
      // determine the key from the calling class and line number
      realKey = ClassUtils.getCallingClass(CALLER_DEPTH);
    }

    // Note: sample could be lost if we get the SamplesMessage but fail to
    //       add the sample before the SamplesMessage is emitted.
    //       This approach avoids synchronization though.
    SamplesMessage realValue = this.samples.get(realKey);
    if(realValue == null) {
      // create the counter if doesn't exist
      SamplesMessage newValue =
          new SamplesMessage(realKey, trackingTypeValue, samplesMaxCount);
      realValue = this.samples.putIfAbsent(realKey, newValue);
      if(realValue == null) {
        realValue = newValue;
      }
    }
    // update the counter
    realValue.addSample(value);
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
    this.decrement(StatType.Counter, key, 1);
  }

  /**
   * Decrements the specified counter by the value specified.
   * @param key the name of the counter to decrement
   * @param value the amount to decrement the counter by
   */
  public void decrement(String key, int value) {
    this.decrement(StatType.Counter, key, value);
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
    this.setKey(StatType.Gauge, key, value);
  }

  /**
   * Sets the counter to the specified value.
   * @param key the name of the counter key to set
   * @param value the value to set this counter to
   */
  public void setKey(String key, long value) {
    this.setKey(StatType.Gauge, key, value);
  }

  public void setKey(StatType type, String key, long value) {
    String realKey = key;

    if(realKey == null) {
      // determine the key from the calling class and line number
      realKey = ClassUtils.getCallingClass(CALLER_DEPTH);
    }

    // create the HashMap if it doesn't exist
    if(this.stats == null) {
      this.stats = new ConcurrentHashMap<>();
    }

    // create and set the gauge counter, this will overwrite the counter
    // if it already exists
    StatsMessage realValue = new StatsMessage(realKey, type);
    realValue.setCounter(value);
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
      List<Context> contextsList = new ArrayList<>(context.size() + 3);
      contextsList.add(new Context(OWNER_KEY, owner));
      contextsList.add(new Context(TRACE_KEY, traceId));
      contextsList.add(new Context(MESSAGE_KEY, message));
      for (Entry<String, String> entry : context.entrySet()) {
        if (OWNER_KEY.equals(entry.getKey()) ||
            TRACE_KEY.equals(entry.getKey()) ||
            MESSAGE_KEY.equals(entry.getKey())) {
          // skip anything set in the context with the arguments
          continue;
        }
        contextsList.add(new Context(entry.getKey(), entry.getValue()));
      }
      Context[] contexts = contextsList.toArray(new Context[0]);

      for (Transport t : transports.get(EventType.TRACE)) {
        try {
          t.sendTrace(programId, contexts);
        } catch (TransportException te) {
          errorHandler.handleError("Error calling Transport.sendTrace()", te);
        }
      }

      ret = true;
    } catch(Exception e) {
      errorHandler.handleError("Error calling Client.traceMessage()", e);
    }

    return ret;
  }

  /**
   * Send a performance trace message.
   * @see <a href="https://github.com/mondemand/mondemand.github.com/blob/master/performance_monitoring.md">Performance Monitoring</a>
   * @param id the performance trace id
   * @param callerLabel the label of the caller, used to give a directed graph
   *                    of performance timings
   * @param label an array of service labels, should equal length of start and
   *              end arrays
   * @param start an array of start times
   * @param end an array of end times
   * @param context a map of contextual metadata
   */
  public boolean performanceTraceMessage(String id, String callerLabel,
                                         String[] label, long[] start,
                                         long[] end,
                                         Map<String, String> context) {
    boolean ret = false;

    try {
      List<Context> contextList = new ArrayList<>();

      for (Entry<String,String> entry : context.entrySet()) {
        contextList.add(new Context(entry.getKey(), entry.getValue()));
      }

      Context[] contexts = contextList.toArray(new Context[0]);

      for (Transport t : transports.get(EventType.PERF)) {
        try {
          t.sendPerformanceTrace(id, callerLabel, label, start, end,
                                 contexts);
        } catch (TransportException te) {
          errorHandler.handleError("Error calling Transport.sendPerformanceTrace()",
                                   te);
        }
      }

      ret = true;
    } catch (Exception e) {
      errorHandler.handleError("Error calling Client.performanceTraceMessage()", e);
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
          LogMessage msg = this.messages.get(key);
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
   * @param messagesToEmit messages to emit
   * @param currentContext current context for the logs
   */
  private synchronized void dispatchLogs(
      ConcurrentHashMap<String,LogMessage> messagesToEmit,
      Context[] currentContext) {
    try {
      LogMessage[] messages = messagesToEmit.values().toArray(new LogMessage[0]);

      for (Transport t : transports.get(EventType.LOG)) {
        try {
          t.sendLogs(programId, messages, currentContext);
        } catch (TransportException te) {
          errorHandler.handleError("Error calling Transport.sendLogs()", te);
        }
      }
    } catch (Exception e) {
      errorHandler.handleError("Error calling Client.dispatchLogs()", e);
    }
  }

  /**
   * Iterates through the transports, calling the send() method for each to send
   * all the stats and samples.
   * Since we cannot assume transports are thread-safe, we make this method synchronized.
   * @param statsToEmit counter stats to emit
   * @param samplesToEmit sample stats to emit
   * @param currentContext current context for the stats
   */
  private synchronized void dispatchStatsSamples(
      StatsMessage[] statsToEmit,
      SamplesMessage[] samplesToEmit,
      Context[] currentContext)
  {
    if ((statsToEmit == null || statsToEmit.length == 0) &&
        (samplesToEmit == null || samplesToEmit.length == 0))
    {
      return;
    }

    try {
      for (Transport t : transports.get(EventType.STATS)) {
        try {
          t.send(programId, statsToEmit, samplesToEmit, currentContext, this.maxNumMetrics);
        } catch (TransportException te) {
          errorHandler.handleError("Error calling Transport.sendStats()", te);
        }
      }
    } catch (Exception e) {
      errorHandler.handleError("Error calling Client.dispatchStats()", e);
    }
  }

  /**
   * emit the events
   * @param contextStatsToEmit context stats to emit
   * @param currentContext current context for the stats
   */
  private synchronized void dispatchContextStats(
      ConcurrentHashMap<ContextList, AtomicLongMap<String>> contextStatsToEmit,
      Context[] currentContext)
  {
    if (contextStatsToEmit == null || contextStatsToEmit.isEmpty()) {
      return;
    }

    for (Map.Entry<ContextList, AtomicLongMap<String>> entry : contextStatsToEmit.entrySet())
    {
      List<Context> newContexts = new ArrayList<>(currentContext.length + entry.getKey().getList().size());
      Collections.addAll(newContexts, currentContext);
      newContexts.addAll(entry.getKey().getList());
      List<StatsMessage> statsMsgs = new ArrayList<>();

      for (Map.Entry<String, Long> stat : entry.getValue().asMap().entrySet())
      {
        StatsMessage statsMessage = new StatsMessage(stat.getKey(), StatType.Counter);
        statsMessage.incrementBy(stat.getValue().intValue());
        statsMsgs.add(statsMessage);
      }

      Context[] contexts = newContexts.toArray(new Context[0]);

      for (Transport t : transports.get(EventType.STATS)) {
        try {
          t.send(programId, statsMsgs.toArray(new StatsMessage[0]), null,
                 contexts, this.maxNumMetrics);
        } catch(TransportException te) {
          errorHandler.handleError("Error calling Transport.sendStats()",
                                   te);
        }
      }
    }
  }

  public ConcurrentHashMap<ContextList, AtomicLongMap<String>> getContextStats() {
    return contextStats;
  }

  public ConcurrentHashMap<String, SamplesMessage> getSamples() {
    return samples;
  }
}
