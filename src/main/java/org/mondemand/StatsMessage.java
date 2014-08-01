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
import java.util.ArrayList;
import java.util.Random;

import org.mondemand.StatType;

public class StatsMessage implements Serializable {
  private static final long serialVersionUID = 6151047586994516863L;

  private String key = null;
  private StatType type = StatType.Unknown;
  private long counter = 0;

  // the following attributes are used for timer counters where we may want to
  // keep more stats like min/max/98 percentile/....
  private ArrayList<Integer> samples = null;          // a sample of entries for timer counter
  public static final int MAX_SAMPLES_COUNT = 1000;   // max number of sample entries to keep
  private long timerCounter = 0;        // counter for timers, we need a separate
                                        // counter since timer stats are gauge.
  private int timerUpdateCounts = 0;    // number of times counter is updated
  private int trackingTypeValue = 0;    // bitwise value to specify what extra
                                        // stats to keep for a timer counter
  Random rand = new Random();

  /**
   * constructor
   * @param key - counter's key
   * @param type - counter's type
   * @param trackingTypeValue - bitwise value, specifies what extra stats
   *        (min/max/...) should be kept for a timer counter, ignored for
   *        non-timer counters.
   */
  public StatsMessage(String key, StatType type, int trackingTypeValue) {
    this.key = key;
    this.type = type;
    // tracking type value and samples are only applicable to timer counters
    if(type == StatType.Timer) {
      this.trackingTypeValue = trackingTypeValue;
      samples = new ArrayList<Integer>(MAX_SAMPLES_COUNT);
    }
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
    return counter;
  }

  /**
   * @return the tracking type value for the counter, 0 for non timer type counters
   */
  public int getTrackingTypeValue() {
    return trackingTypeValue;
  }

  /**
   * @return samples for timer counters
   */
  public ArrayList<Integer> getSamples() {
    return samples;
  }

  /**
   * @return number of times this counter has been updated since last emission.
   */
  public int getTimerUpdateCounts() {
    return timerUpdateCounts;
  }

  /**
   * @return counter for a timer stat since last emission
   */
  public long getTimerCounter() {
    return timerCounter;
  }

  /**
   * increments the counter by some value, replaces setCounter()
   * @param value - value to increment by
   */
  public void incrementBy(int value) {
    // synchronize on this object so it won't be updated while another
    // thread is sending this instance's stats
    synchronized(this) {
      counter += value;
      // update samples for timer counters
      if(type == StatType.Timer) {
        timerCounter += value;
        timerUpdateCounts++;
        if(samples.size() < MAX_SAMPLES_COUNT) {
          // add new value to samples if it has space
          samples.add(value);
        } else {
          // otherwise, replace one of the entries with the new value
          // with the probability of "MAX_SAMPLES_COUNT / timerUpdateCounts"
          int indexToReplace = rand.nextInt(timerUpdateCounts);   // from 0 to timerUpdateCounts-1
          if( indexToReplace < MAX_SAMPLES_COUNT) {
            samples.set(indexToReplace, value);
          }
        }
      }
    }
  }

  /**
   * reset the timer-stat specific attributes of this object.
   * this method is called after emission of a timer stat
   */
  public void resetSamples() {
    if(type == StatType.Timer) {
      samples.clear();
      timerCounter = 0;
      timerUpdateCounts = 0;
    }
  }

  /**
   * @param counter the counter to set
   */
  public void setCounter(long counter) {
    this.counter = counter;
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

  public String toString() {
    return type + " : " + key + " : " + counter;
  }
}
