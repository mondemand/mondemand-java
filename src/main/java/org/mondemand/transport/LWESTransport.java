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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;

import org.mondemand.Context;
import org.mondemand.LogMessage;
import org.mondemand.SamplesMessage;
import org.mondemand.StatType;
import org.mondemand.StatsMessage;
import org.mondemand.SampleTrackType;
import org.mondemand.TraceId;
import org.mondemand.Transport;
import org.mondemand.TransportException;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.emitter.EventEmitter;
import org.lwes.emitter.MulticastEventEmitter;
import org.lwes.emitter.UnicastEventEmitter;

public class LWESTransport
  implements Transport
{
  /********************
   * Constants        *
   ********************/
  private static final String LOG_EVENT   = "MonDemand::LogMsg";
  private static final String STATS_EVENT = "MonDemand::StatsMsg";
  private static final String TRACE_EVENT = "MonDemand::TraceMsg";

  /***********************
   * Instance attributes *
   ***********************/
  private EventEmitter emitter = null;

  /**
   * Creates an initializes a LWES transport.
   * @param address the address to send events to, can be a multicast
   *                or unicast address
   * @param port the port to send events to
   * @param networkInterface the network interface to use, or null to
   *                         specify the default
   * @throws TransportException
   */
  public LWESTransport (InetAddress address,
                        int port,
                        InetAddress networkInterface)
    throws TransportException
  {
    this(address, port, networkInterface, 1);
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
  public LWESTransport (InetAddress address,
                        int port,
                        InetAddress networkInterface,
                        int ttl)
    throws TransportException
  {
    if(address == null) return;

    // detect the address type and initialize accordingly
    if(address.isMulticastAddress()) {
      MulticastEventEmitter m = new MulticastEventEmitter();
      m.setMulticastAddress(address);
      m.setMulticastPort(port);
      m.setInterface(networkInterface);
      m.setTimeToLive(ttl);
      emitter = m;
    } else {
      UnicastEventEmitter u = new UnicastEventEmitter();
      u.setAddress(address);
      u.setPort(port);
      emitter = u;
    }

    try {
      emitter.initialize();
    } catch(Exception e) {
      throw new TransportException("Unable to inialize emitter", e);
    }
  }

  public void sendLogs (String programId,
                        LogMessage[] messages,
                        Context[] contexts)
    throws TransportException
  {
    if (messages == null || messages.length == 0
        || contexts == null || emitter == null) return;

    try {
      // create the event and set parameters
      Event logMsg = emitter.createEvent(LOG_EVENT, false);
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
      emitter.emit(logMsg);
    } catch(EventSystemException e) {
      throw new TransportException("Error sending log event", e);
    } catch(IOException ie) {
      throw new TransportException("Error sending log event", ie);
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
  public void send(String programId, StatsMessage[] stats,
      SamplesMessage[] samples, Context[] contexts) throws TransportException {

    // create the event
    Event event = emitter.createEvent(STATS_EVENT, false);
    event.setString("prog_id", programId);

    int idx=0;
    idx = sendStats(event, stats, contexts, idx);
    idx = sendSamples(event, samples, idx);

    // number of entries in the event is the total of stats and samples
    event.setUInt16("num", idx);

    // finally emit the event
    try {
      emitter.emit(event);
    } catch(Exception e) {
      throw new TransportException("Error sending log event", e);
    }

  }

  /**
   * sends all the stats
   * @param event - event to set the stats in
   * @param messages - stats
   * @param contexts - contexts
   * @param idx - beginning index for the entries in the event
   * @return index of last entry added to the event
   */
  public int sendStats (Event event, StatsMessage[] messages,
      Context[] contexts, int idx) {
    if (messages == null || messages.length == 0
        || contexts == null || emitter == null)
      return idx;

    // for each statistic, set the values
    for(StatsMessage msg: messages) {
      synchronized(msg) {
        // synchronize on msg to make sure some other threads are not updating it
        // at the same time, otherwise there may be exception during sorting
        setLwesEvent(event, msg.getType().toString(), msg.getKey(),
            msg.getCounter(), idx);
        idx++;
      }
    }

    // set the contextual data in the event
    if(contexts.length > 0) {
      event.setUInt16("ctxt_num", contexts.length);
      for(int i=0; i<contexts.length; ++i) {
        event.setString("ctxt_k" + i, contexts[i].getKey());
        event.setString("ctxt_v" + i, contexts[i].getValue());
      }
    }

    return idx;
  }

  /**
   * sends all the samples
   * @param event - event to set the samples in
   * @param messages - samples
   * @param contexts - contexts
   * @param idx - beginning index for the entries in the event
   * @return index of last entry added to the event
   */
  public int sendSamples (Event event, SamplesMessage[] messages,
      int idx) {
    if (messages == null || messages.length == 0 || emitter == null) {
      return idx;
    }

    // for each statistic, set the values
    for(SamplesMessage msg: messages) {
      synchronized(msg) {
        // synchronize on msg to make sure some other threads are not updating it
        // at the same time, otherwise there may be exception during sorting
        // add messages for extra stats for timer stat
        idx = updateLwesEventForTimers(event, msg, idx);
      }
    }

    return idx;
  }


  /**
   * sets a type/key/value in an lwes event object's message part
   * @param statsMsg - the lwes event object
   * @param type - the event's type
   * @param key - the event's key
   * @param value - the event's value
   * @param index - the index used for the type/key/value
   */
  protected void setLwesEvent(Event statsMsg, String type, String key, long value,
      int index) {
    statsMsg.setString("t" + index, type);
    statsMsg.setString("k" + index, key);
    statsMsg.setInt64("v" + index, value);
  }

  /**
   * update an lwes event with the extra stats (min/max/...) for a timer stat.
   *
   * @param statsMsg - the lwes event to be updated
   * @param msg - the StatsMessage object to update the event
   * @param index - the index to be incremented and used to lwes event
   * @return the last index that was added in this method.
   */
  protected int updateLwesEventForTimers(Event event, SamplesMessage msg,
      int index) {
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
            if(trackType.value == SampleTrackType.AVG.value) {
              // value for average is not coming from sortedSamples
              value = msg.getCounter()/msg.getUpdateCounts();
            } else {
              value = sortedSamples.get((int)( (sortedSamples.size() - 1) * trackType.indexInSamples));
            }
          } else {
            // samples were not updated, i.e., no increment since the last
            // emit, so send a value of 0
          }
          // "min_", "max_", ... will be added to the original key
          // all these stats are gauges.
          setLwesEvent(event, StatType.Gauge.toString(),
                msg.getKey() + trackType.keySuffix, value, index);
          index++;
        }
      }
    }
    // clear the samples, and counters for samples messages
    msg.resetSamples();
    return index;
  }

  private static final String PROG_ID_KEY  = "mondemand.prog_id";
  private static final String SRC_HOST_KEY = "mondemand.src_host";

  public void sendTrace (String programId,
                         Context[] contexts)
    throws TransportException
  {
    if (contexts == null || emitter == null)
      return;

    try {
      Event traceMsg = emitter.createEvent(TRACE_EVENT, false);
      traceMsg.setString(PROG_ID_KEY, programId);
      String hostName = InetAddress.getLocalHost ().getHostName ();
      traceMsg.setString(SRC_HOST_KEY, hostName);

      for(int i=0; i<contexts.length; ++i) {
          traceMsg.setString(contexts[i].getKey(), contexts[i].getValue());
      }
      // emit the event
      emitter.emit(traceMsg);
    } catch(Exception e) {
      throw new TransportException("Error sending log event", e);
    }
  }


  public void shutdown()
    throws TransportException
  {
    try {
      emitter.shutdown();
    } catch(Exception e) {
      throw new TransportException("Unable to shutdown emitter");
    }
  }
}
