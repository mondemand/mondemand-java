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

public interface Transport {
  public void sendLogs(String programId, LogMessage[] messages,
                       Context[] contexts)
    throws TransportException;

  public void send(String programId, StatsMessage[] stats,
                   SamplesMessage[] samples, Context[] contexts)
    throws TransportException;

  public void sendTrace(String programId, Context[] contexts)
    throws TransportException;

  public void sendPerformanceTrace(String id, String callerLabel,
                                   String[] label, long[] start,
                                   long[] end, Context[] contexts)
    throws TransportException;

  public void shutdown() throws TransportException;
}
