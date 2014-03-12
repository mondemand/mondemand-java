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

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.emitter.EventEmitter;
import org.lwes.emitter.MulticastEventEmitter;
import org.lwes.emitter.UnicastEventEmitter;
import org.mondemand.Context;
import org.mondemand.LogMessage;
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
  private static final String STATS_EVENT = "MonDemand::StatsMsg";
  private static final String TRACE_EVENT = "MonDemand::TraceMsg";

  /***********************
   * Instance attributes *
   ***********************/
  private EventEmitter emitter = null;
  private HttpClient httpClient = null;
  private String tcpEndpoint = null;
  private boolean tcpTransportAvailable = false;
  
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

  public LWESTransport (InetAddress address, int udpPort, int tcpPort, String endpoint,
                        InetAddress networkInterface, int ttl)
    throws TransportException
  {
      this (address, udpPort, networkInterface, ttl);
      
      this.tcpEndpoint = constructTcpUrl(address, tcpPort, endpoint);
      PoolingHttpClientConnectionManager cm = 
              new PoolingHttpClientConnectionManager();
      this.httpClient = HttpClients.custom().setConnectionManager(cm).build();
      this.tcpTransportAvailable = true;
  }
  
  private String constructTcpUrl(InetAddress address, int tcpPort,
            String endpoint) {
      return new StringBuilder("http://").append(address.getHostAddress())
             .append(":").append(tcpPort).append("/").append(endpoint)
             .toString();
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

  public void sendStats (String programId,
                         StatsMessage[] messages,
                         Context[] contexts)
    throws TransportException
  {
    if (messages == null || messages.length == 0
        || contexts == null || emitter == null)
      return;

    try {
      // create the event
      Event statsMsg = emitter.createEvent(STATS_EVENT, false);
      statsMsg.setString("prog_id", programId);
      statsMsg.setUInt16("num", messages.length);

      // for each statistic, set the values
      for(int i=0; i<messages.length; ++i) {
        statsMsg.setString("t" + i, messages[i].getType().toString());
        statsMsg.setString("k" + i, messages[i].getKey());
        statsMsg.setInt64("v" + i, messages[i].getCounter());
      }

      // set the contextual data in the event
      if(contexts.length > 0) {
        statsMsg.setUInt16("ctxt_num", contexts.length);
        for(int i=0; i<contexts.length; ++i) {
          statsMsg.setString("ctxt_k" + i, contexts[i].getKey());
          statsMsg.setString("ctxt_v" + i, contexts[i].getValue());
        }
      }

      // emit the event
      emitter.emit(statsMsg);
    } catch(Exception e) {
      throw new TransportException("Error sending log event", e);
    }
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
      boolean needTcpTransport = false;
      Event traceMsg = emitter.createEvent(TRACE_EVENT, false);
      traceMsg.setString(PROG_ID_KEY, programId);
      String hostName = InetAddress.getLocalHost ().getHostName ();
      traceMsg.setString(SRC_HOST_KEY, hostName);

      for(int i=0; i<contexts.length; ++i) {
          if(longString(contexts[i].getValue())){
              traceMsg.setLongString(contexts[i].getKey(), contexts[i].getValue());
              needTcpTransport = true;
          }
          else
              traceMsg.setString(contexts[i].getKey(), contexts[i].getValue());
      }
      // emit the event
      if (needTcpTransport && tcpTransportAvailable)
          postViaHttp (traceMsg);
      else
          emitter.emit(traceMsg);
    } catch(Exception e) {
      throw new TransportException("Error sending log event", e);
    }
  }

  private boolean longString(String str) {
      return StringUtils.length(str) > 65535 ? true : false;
  }
  
  private void postViaHttp(Event e) throws ClientProtocolException,
          IOException {
      byte[] bin = e.serialize();
      HttpPost post = new HttpPost(tcpEndpoint);
      post.setEntity(new ByteArrayEntity(bin));
      HttpResponse response = httpClient.execute(post);
      EntityUtils.consume(response.getEntity());
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
