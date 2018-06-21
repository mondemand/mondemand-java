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

package org.mondemand.transport;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import org.lwes.Event;
import org.lwes.EventFactory;
import org.lwes.EventSystemException;
import org.lwes.emitter.EmitterGroup;
import org.lwes.emitter.EmitterGroupBuilder;
import org.mondemand.Config;
import org.mondemand.Context;
import org.mondemand.LogMessage;
import org.mondemand.SampleTrackType;
import org.mondemand.SamplesMessage;
import org.mondemand.StatType;
import org.mondemand.StatsMessage;
import org.mondemand.TraceId;
import org.mondemand.Transport;
import org.mondemand.TransportException;

public class LWESTransport
  implements Transport
{

  /********************
   * Constants        *
   ********************/
  private static final String LOG_EVENT   = "MonDemand::LogMsg";
  private static final String PERF_EVENT  = "MonDemand::PerfMsg";
  private static final String STATS_EVENT = "MonDemand::StatsMsg";
  private static final String TRACE_EVENT = "MonDemand::TraceMsg";
  private static final int DEFAULT_MAXIMUM_METRICS = 512;

  /***********************
   * Instance attributes *
   ***********************/
  private EmitterGroup emitterGroup = null;

  /**
   * Creates an initializes a LWES transport.
   * @param address the address to send events to, can be a multicast
   *                or unicast address
   * @param port the port to send events to
   * @param networkInterface the network interface to use, or null to
   *                         specify the default
   * @throws TransportException
   */
  public LWESTransport(InetAddress address,
                       int port,
                       InetAddress networkInterface)
    throws TransportException
  {
    this(address, port, networkInterface, Config.TTL_DEFAULT);
  }

  /**
   * Creates and initializes a LWES transport.
   * @param address the address to send events to, can be a multicast
   *                or unicast address
   * @param port the port to send events to
   * @param networkInterface the network interface to use, or null to
   *                         specify the default
   * @param ttl for multicast addresses, the TTL value to use
   * @throws TransportException
   */
  public LWESTransport(InetAddress address,
                       int port,
                       InetAddress networkInterface,
                       int ttl)
    throws TransportException
  {
    this(new Config(
        address, networkInterface, port, ttl, null).toEmitterGroupProperties(
            address.getHostAddress()), address.getHostAddress());
  }

  /**
   * Creates and initializes a LWES transport using an emitter group config.
   * @param emitterGroupProps properties of the emitter group
   * @param emitterGroupName name of the emitter group
   * @throws TransportException
   */
  public LWESTransport(Properties emitterGroupProps, String emitterGroupName)
    throws TransportException
  {
    try {
      emitterGroup =
        EmitterGroupBuilder.createGroup(emitterGroupProps, emitterGroupName,
                                        new EventFactory());
    } catch (Exception e) {
      throw new TransportException("Unable to initialize emitter group", e);
    }
  }

  @Override
  public void sendLogs (String programId,
                        LogMessage[] messages,
                        Context[] contexts)
    throws TransportException
  {
    if (messages == null || messages.length == 0
        || contexts == null || emitterGroup == null) {
      return;
    }

    try {
      // create the event and set parameters
      Event logMsg = emitterGroup.createEvent(LOG_EVENT, false);
      logMsg.setString("prog_id", programId);
      logMsg.setUInt16("num", messages.length);

      // for each log message, set the appropriate fields
      for(int i=0; i<messages.length; ++i) {
        TraceId traceId = messages[i].getTraceId();
        if(traceId != null) {
          if(traceId.compareTo(TraceId.NULL_TRACE_ID) != 0) {
            logMsg.setUInt64 ("trace_id" + i, traceId.getId());
          }
        }

        logMsg.setString("f" + i, messages[i].getFilename());
        logMsg.setUInt32("l" + i, messages[i].getLine());
        logMsg.setUInt32("p" + i, messages[i].getLevel());
        logMsg.setString("m" + i, messages[i].getMessage());

        if(messages[i].getRepeat() > 1) {
          logMsg.setUInt16("r" + i, messages[i].getRepeat());
        }
      }

      // set the contextual data in the event
      if(contexts.length > 0) {
        logMsg.setUInt16("ctxt_num", contexts.length);
        for(int i=0; i<contexts.length; ++i) {
          logMsg.setString("ctxt_k" + i, contexts[i].getKey());
          logMsg.setString("ctxt_v" + i, contexts[i].getValue());
        }
      }

      // emit the event
      emitterGroup.emitToGroup(logMsg);
    } catch(EventSystemException e) {
      throw new TransportException("Error sending log event", e);
    }
  }

  /**
   * sends all the stats and samples
   * @param programId - program id
   * @param stats - stats
   * @param samples - samples
   * @param contexts - contexts
   * @throws TransportException
   */
  @Override
  public void send(String programId, StatsMessage[] stats,
      SamplesMessage[] samples, Context[] contexts, Integer maxNumMetrics)
    throws TransportException
  {
    if (contexts == null || emitterGroup == null)
    {
      return;
    }

    try
    {
      StatsMessageStreamer sms =
        new StatsMessageStreamer(programId, contexts, emitterGroup,
            (null == maxNumMetrics) ? DEFAULT_MAXIMUM_METRICS : maxNumMetrics);

      sendStats(sms, stats);
      sendSamples(sms, samples);
      sms.flush();
    }
    catch (EventSystemException e)
    {
      throw new TransportException("Error sending stats event", e);
    }

  }

  /**
   * sends all the stats
   * @param sms - the StatsMessageStreamer to be updated
   * @param messages - stats
   */
  public void sendStats (StatsMessageStreamer sms, StatsMessage[] messages) {
    if (messages == null || messages.length == 0)
    {
      return;
    }

    // for each statistic, set the values
    for(StatsMessage msg: messages) {
      synchronized(msg) {
        // synchronize on msg to make sure some other threads are not updating it
        // at the same time, otherwise there may be exception during sorting
        sms.addMetric(msg.getType().toString(), msg.getKey(),
            msg.getCounter());
      }
    }
  }

  /**
   * sends all the samples
   * @param sms - the StatsMessageStreamer to be updated
   * @param messages - samples
   */
  public void sendSamples (StatsMessageStreamer sms, SamplesMessage[] messages) {
    if (messages == null || messages.length == 0)
    {
      return;
    }

    // for each statistic, set the values
    for(SamplesMessage msg: messages) {
      synchronized(msg) {
        // synchronize on msg to make sure some other threads are not updating it
        // at the same time, otherwise there may be exception during sorting
        // add messages for extra stats for samples
        updateLwesEventForSamples(sms, msg);
      }
    }
  }

  /**
   * update an lwes event with the extra stats (min/max/...) for a sample message.
   *
   * @param sms - the StatsMessageStreamer to be updated
   * @param msg - the StatsMessage object to update the event
   */
  protected void updateLwesEventForSamples(StatsMessageStreamer sms, SamplesMessage msg) {
    // already synchronized on msg in the calling method
    if(msg.getTrackingTypeValue() > 0) {
      // first sort the samples
      ArrayList<Integer> sortedSamples = msg.getSamples();
      Collections.sort(sortedSamples);

      // go through all the trackTypes and if one is set for the counter, emit that
      for(SampleTrackType trackType: SampleTrackType.values()) {
        if( (msg.getTrackingTypeValue() & trackType.value) == trackType.value) {
          // default value (in case samples were not updated since last emit)
          long value = 0;
          if(sortedSamples.size() > 0) {
            // values for average, sum and count are not coming from sortedSamples
            if(trackType.value == SampleTrackType.AVG.value) {
              value = msg.getCounter()/msg.getUpdateCounts();
            } else if(trackType.value == SampleTrackType.SUM.value) {
              value = msg.getCounter();
            } else if(trackType.value == SampleTrackType.COUNT.value) {
              value = msg.getUpdateCounts();
            } else {
              value = sortedSamples.get((int)( (sortedSamples.size() - 1) * trackType.indexInSamples));
            }
          } else {
            // samples were not updated, i.e., no increment since the last
            // emit, so send a value of 0
          }
          // "_min", "_max", ... will be added to the original key
          // all these stats are gauges.
          sms.addMetric(StatType.Gauge.toString(),
                msg.getKey() + trackType.keySuffix, value);
        }
      }
    }

    return;
  }

  /**
   * StatsMessageStreamer is a class that will take care of sending the lwes
   * events that contain all stats and samples metrics, ensuring that each event
   * does not contain more than MAXIMUM_METRICS metrics.  Metrics are added by
   * calling addMetric, and a call to flush should be done once all metrics have
   * been added.
   */
  class StatsMessageStreamer {
    final String programId;
    Context[] contexts;
    int maxMetrics;
    int numMetrics = 0;
    Event statsMsg;
    EmitterGroup emitterGroup;

    StatsMessageStreamer (String programId, Context[] contexts, EmitterGroup emitterGroup, Integer maxMetrics)
      throws EventSystemException
    {
      this.programId = programId;
      this.contexts = contexts;
      this.emitterGroup = emitterGroup;
      this.maxMetrics = maxMetrics;

      initializeEvent();
    }

    /**
     * create an event instance and reset numMetrics
     */
    private void initializeEvent ()
      throws EventSystemException
    {
      statsMsg = emitterGroup.createEvent(STATS_EVENT, false);
    }

    /**
     * add any keys that are expected to be in each event and the context, and
     * emit the event.
     */
    private void emitMessage ()
    {
      statsMsg.setString("prog_id", programId);
      statsMsg.setUInt16("num", numMetrics);
      int contextCount = 0;
      for (Context context : contexts) {
        statsMsg.setString("ctxt_k" + contextCount, context.getKey());
        statsMsg.setString("ctxt_v" + contextCount, context.getValue());
        ++contextCount;
      }
      statsMsg.setUInt16("ctxt_num", contextCount);

      emitterGroup.emitToGroup(statsMsg);

      numMetrics = 0;
    }

    /**
     * flush any remaining data
     */
    void flush()
    {
      /*
       * only emit if there are 1 or more metrics.  Is there any reason to send
       * an event that has only context data and no metrics?
       */
      if (numMetrics > 0)
      {
        emitMessage();
      }
    }

    /**
     * sets a type/key/value in an lwes event object's message part
     * @param type - the event's type
     * @param key - the event's key
     * @param value - the event's value
     */
    void addMetric(String type, String key, long value)
    {
      if (numMetrics == maxMetrics)
      {
        // this resets numMetrics to 0
        emitMessage();
        initializeEvent();
      }
      statsMsg.setString("t" + numMetrics, type);
      statsMsg.setString("k" + numMetrics, key);
      statsMsg.setInt64("v" + numMetrics, value);
      numMetrics++;
    }
  }

  private static final String PROG_ID_KEY  = "mondemand.prog_id";
  private static final String SRC_HOST_KEY = "mondemand.src_host";

  @Override
  public void sendTrace (String programId,
                         Context[] contexts)
    throws TransportException
  {
    if (contexts == null || emitterGroup == null) {
      return;
    }

    try {
      Event traceMsg = emitterGroup.createEvent(TRACE_EVENT, false);
      traceMsg.setString(PROG_ID_KEY, programId);
      String hostName = InetAddress.getLocalHost ().getHostName ();
      traceMsg.setString(SRC_HOST_KEY, hostName);

      for(int i=0; i<contexts.length; ++i) {
          traceMsg.setString(contexts[i].getKey(), contexts[i].getValue());
      }
      // emit the event
      emitterGroup.emitToGroup(traceMsg);
    } catch(Exception e) {
      throw new TransportException("Error sending log event", e);
    }
  }

  private static final String PERF_ID_KEY = "id";
  private static final String PERF_CALLER_LABEL_KEY = "caller_label";
  private static final String PERF_NUM_KEY = "num";
  private static final String PERF_LABEL_PREFIX = "label";
  private static final String PERF_START_PREFIX = "start";
  private static final String PERF_END_PREFIX = "end";

  @Override
  public void sendPerformanceTrace(String id, String callerLabel,
                                   String[] label, long[] start,
                                   long[] end, Context[] contexts)
    throws TransportException
  {
    if (id == null || callerLabel == null || label == null || start == null ||
        end == null || contexts == null)
    {
      throw new IllegalArgumentException("missing required argument");
    }

    if (label.length != start.length || label.length != end.length)
    {
      throw new IllegalArgumentException("label, start, and end arrays must " +
                                         "all be of equal length");
    }

    try {
      Event perfMsg = emitterGroup.createEvent(PERF_EVENT, false);

      perfMsg.setString(PERF_ID_KEY, id);
      perfMsg.setString(PERF_CALLER_LABEL_KEY, callerLabel);
      perfMsg.setUInt16(PERF_NUM_KEY, label.length);

      for (int i = 0; i < label.length; ++i) {
        perfMsg.setString(PERF_LABEL_PREFIX + i, label[i]);
        perfMsg.setInt64(PERF_START_PREFIX + i, start[i]);
        perfMsg.setInt64(PERF_END_PREFIX + i, end[i]);
      }

      // set the contextual data in the event
      if (contexts.length > 0) {
        perfMsg.setUInt16("ctxt_num", contexts.length);

        for (int i = 0; i < contexts.length; ++i) {
          perfMsg.setString("ctxt_k" + i, contexts[i].getKey());
          perfMsg.setString("ctxt_v" + i, contexts[i].getValue());
        }
      }

      // emit the event
      emitterGroup.emitToGroup(perfMsg);
    } catch(Exception e) {
      throw new TransportException("Error sending perf event", e);
    }
  }

  @Override
  public void shutdown()
    throws TransportException
  {
    try {
      emitterGroup.shutdown();
    } catch(Exception e) {
      throw new TransportException("Unable to shutdown emitter group");
    }
  }
}
