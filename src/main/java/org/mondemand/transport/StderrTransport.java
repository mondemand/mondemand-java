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

import java.util.ArrayList;
import java.util.Collections;

import org.mondemand.Context;
import org.mondemand.Level;
import org.mondemand.LogMessage;
import org.mondemand.SampleTrackType;
import org.mondemand.SamplesMessage;
import org.mondemand.StatType;
import org.mondemand.StatsMessage;
import org.mondemand.Transport;

public class StderrTransport implements Transport {

  public StderrTransport() {
  }

  public void sendLogs (String programId,
                        LogMessage[] messages,
                        Context[] contexts)
  {
    if(messages == null) return;

    for(int i=0; i<messages.length; ++i) {
      try {
        if (messages[i].getTraceId() != null) {
          System.err.println("["+Level.STRINGS[messages[i].getLevel()]+"] - "+
              messages[i].getFilename()  + ":" + messages[i].getLine() + " " +
              "[" + messages[i].getTraceId() + "] " +
              messages[i].getMessage() + "(" + messages[i].getRepeat() + ")");
        } else {
          System.err.println("["+Level.STRINGS[messages[i].getLevel()]+"] - " +
              messages[i].getFilename()  + ":" + messages[i].getLine() + " " +
              messages[i].getMessage() + "(" + messages[i].getRepeat() + ")");
        }
      } catch(Exception e) {}
    }
  }

  public void send (String programId,
      StatsMessage[] stats,
      SamplesMessage[] samples,
      Context[] contexts) {
   sendStats(programId, stats, contexts);
   sendSamples(programId, samples);
  }

  public void sendStats (String programId,
                         StatsMessage[] messages,
                         Context[] contexts)
  {
    if(messages == null) return;

    for(int i=0; i<messages.length; ++i) {
      try {
        System.err.println("["+programId+"] " + messages[i].getType() + " : "
                                              + messages[i].getKey() + " : "
                                              + messages[i].getCounter());
      } catch(Exception e) {
        // we can't write to stderr, so just give up
      }
    }
  }

  public void sendSamples(String programId, SamplesMessage[] messages) {
    if(messages == null) return;

    for(SamplesMessage msg: messages) {
      if(msg.getTrackingTypeValue() > 0) {
        // first sort the samples
        ArrayList<Integer> sortedSamples = msg.getSamples();
        Collections.sort(sortedSamples);

        // go through all the trackTypes and if one is set for the counter, log that
        for(SampleTrackType trackType: SampleTrackType.values()) {
          if( (msg.getTrackingTypeValue() & trackType.value) == trackType.value) {
            // default value (in case samples were not updated since last log)
            long value = 0;
            if(sortedSamples.size() != 0) {
              if(trackType.value == SampleTrackType.AVG.value) {
                // value for average is not coming from sortedSamples
                value = msg.getCounter()/msg.getUpdateCounts();
              } else {
                value = sortedSamples.get((int)( (sortedSamples.size() - 1) * trackType.indexInSamples));
              }
            } else {
              // samples were not updated, i.e., no increment since the last
              // log, so log a value of 0
            }
            // "min_", "max_", ... will be added to the original key
            System.err.println("["+programId+"] " + msg.getType() + " : "
                + msg.getKey() + trackType.keySuffix + " : "
                + value);
          }
        }
      }
    }
 }

  public void sendTrace (String programId,
                         Context[] contexts)
  {
    try {
      System.err.println ("["+programId+"]");
      for(int i=0; i<contexts.length; ++i) {
        System.err.println (contexts[i].getKey() + " = " + contexts[i].getValue());
      }
    } catch(Exception e) {
      // we can't write to stderr, so just give up
    }
  }

  public void shutdown() {
    // do nothing
  }

}
