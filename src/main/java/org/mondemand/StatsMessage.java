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

import java.io.Serializable;
import java.util.concurrent.atomic.LongAdder;

import org.mondemand.StatType;

public class StatsMessage implements Serializable {
  private static final long serialVersionUID = 6151047586994516863L;

  private String key = null;
  private StatType type = StatType.Unknown;
  private LongAdder counter = new LongAdder();

  /**
   * constructor
   * @param key - counter's key
   * @param type - counter's type
   */
  public StatsMessage(String key, StatType type) {
    this.key = key;
    this.type = type;
  }

  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key the key to set
   */
  @Deprecated
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * @return the counter
   */
  public long getCounter() {
    return counter.sum();
  }

  /**
   * increments the counter by some value, replaces setCounter()
   * @param value - value to increment by
   */
  public void incrementBy(int value) {
    counter.add(value);
  }

  /**
   * @param counter the counter to set
   */
  public void setCounter(long counter) {
    // Not atomic but this should be ok
    this.counter.reset();
    this.counter.add(counter);
  }

  /**
   * @return the type
   */
  public StatType getType() {
    return type;
  }

  /**
   * @param type set the type of the stat
   */
  @Deprecated
  public void setType (StatType type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return type + " : " + key + " : " + counter;
  }
}
