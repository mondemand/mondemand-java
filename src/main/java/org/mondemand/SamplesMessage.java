package org.mondemand;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

/**
 * object for a sample message
 *
 */
public class SamplesMessage implements Serializable  {
  private static final long serialVersionUID = 3447528863504915877L;

  private String key = null;
  private final StatType type = StatType.Gauge;   // stat type for samples is always gauge

  private ArrayList<Integer> samples = null;          // a sample of entries
  public static final int MAX_SAMPLES_COUNT = 1000;   // default max number of sample entries to keep
  private int samplesMaxCount;          // max number of sample entries to keep
  private long counter = 0;             // counter since stats are emitted.
  private int updateCounts = 0;         // number of times the object is updated
  private int trackingTypeValue = 0;    // bitwise value to specify what extra
                                        // stats to keep for a timer counter
  Random rand = new Random();

  /**
   * constructor
   * @param key - counter's key
   * @param trackingTypeValue - bitwise value, specifies what extra stats
   *        (min/max/...) should be kept for a counter
   * @param samplesMaxCount - maximum number of samples to keep, ignored if
   *        less than or equal to 0.
   */
  public SamplesMessage(String key, int trackingTypeValue, int samplesMaxCount) {
    this.key = key;
    this.trackingTypeValue = trackingTypeValue;
    this.samplesMaxCount = (samplesMaxCount <= 0 ? MAX_SAMPLES_COUNT : samplesMaxCount);
    samples = new ArrayList<Integer>(this.samplesMaxCount);
  }

  /**
   * constructor, sample size set to MAX_SAMPLES_COUNT
   * @param key - counter's key
   * @param trackingTypeValue - bitwise value, specifies what extra stats
   *        (min/max/...) should be kept for a counter
   */
  public SamplesMessage(String key, int trackingTypeValue) {
    this(key, trackingTypeValue, MAX_SAMPLES_COUNT);
  }

  /**
   * adds a new sample to the list of samples
   * @param value - value of the sample
   */
  public void addSample(int value) {
    // synchronize on this object so it won't be updated while another
    // thread is sending this instance's stats
    synchronized(this) {
      counter += value;
      updateCounts++;
      if(samples.size() < samplesMaxCount) {
        // add new value to samples if it has space
        samples.add(value);
      } else {
        // otherwise, replace one of the entries with the new value
        // with the probability of "samplesCount / timerUpdateCounts"
        int indexToReplace = rand.nextInt(updateCounts);   // from 0 to timerUpdateCounts-1
        if( indexToReplace < samplesMaxCount) {
          samples.set(indexToReplace, value);
        }
      }
    }
  }

  /**
   * this method should be called after emission of this object
   */
  public void resetSamples() {
    samples.clear();
    counter = 0;
    updateCounts = 0;
  }

  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @return the total count value for this object
   */
  public long getCounter() {
    return counter;
  }

  /**
   * @return samples for this counter
   */
  public ArrayList<Integer> getSamples() {
    return samples;
  }

  /**
   * @return the tracking type value for the counter
   */
  public int getTrackingTypeValue() {
    return trackingTypeValue;
  }

  /**
   * @return number of times this counter has been updated since last emission.
   */
  public int getUpdateCounts() {
    return updateCounts;
  }

  /**
   * @return the maximum number of samples we keep for this object
   */
  public int getSamplesMaxCount() {
    return samplesMaxCount;
  }

  /**
   * @return the type
   */
  public StatType getType() {
    return type;
  }

}
