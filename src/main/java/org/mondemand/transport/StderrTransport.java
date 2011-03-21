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

import org.mondemand.Context;
import org.mondemand.Level;
import org.mondemand.LogMessage;
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
