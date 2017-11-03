package org.mondemand;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * object for a sample message
 *
 */
public class SamplesMessage implements Serializable  {
  private static final long serialVersionUID = 3447528863504915877L;

  private static final SampleTrackType[] SAMPLE_TRACK_TYPE_VALUES = SampleTrackType.values();

  private final String key;
  private final StatType type = StatType.Gauge;   // stat type for samples is always gauge

  public static final int MAX_SAMPLES_COUNT = 1000;   // default max number of sample entries to keep
  private final int samplesMaxCount; // max number of sample entries to keep

  // Data fields. These aren't updated together atomically so there may be
  // discrepancies if the emit occurs during an update
  private final LongAdder counter = new LongAdder(); // counter since stats are emitted.
  private final AtomicInteger updateCounts = new AtomicInteger(); // number of times the object is updated
  private final AtomicIntegerArray samples; // a sample of entries

  private final int trackingTypeValue; // bitwise value to specify what extra
                                       // stats to keep for a sample counter

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
    samples = new AtomicIntegerArray(this.samplesMaxCount);
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
  public boolean addSample(int value) {
    int prevCount = updateCounts.getAndIncrement();
    counter.add(value);
    if (prevCount < samplesMaxCount) {
      // add new value to samples if it has space
      samples.set(prevCount, value);
    } else {
      // otherwise, replace one of the entries with the new value
      // with the probability of "samplesCount / UpdateCounts"
      int indexToReplace = ThreadLocalRandom.current().nextInt(prevCount); // from 0 to UpdateCounts-1
      if (indexToReplace < samplesMaxCount) {
        samples.set(indexToReplace, value);
      }
    }
    return true;
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
    return counter.sum();
  }

  /**
   * @return copy of samples for this counter
   */
  public int[] getSamples() {
    long counts = getUpdateCounts();
    int[] samplesCopy = new int[(int)Math.min(counts, samples.length())];
    for (int i = 0; i < samplesCopy.length; ++i) {
      samplesCopy[i] = samples.get(i);
    }
    return samplesCopy;
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
    return updateCounts.get();
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

  /**
   * Gets the current stats specified by the sample track types specified when object
   * was constructed
   * @return map of stats
   */
  public Map<SampleTrackType, Long> getStats() {
    if (trackingTypeValue == 0) {
      return Collections.emptyMap();
    }

    long counts = getUpdateCounts();
    long total = getCounter();
    int[] sortedSamples = getSamples();
    // first sort the samples
    Arrays.sort(sortedSamples);

    Map<SampleTrackType, Long> stats = new EnumMap<>(SampleTrackType.class);

    for (SampleTrackType trackType : SAMPLE_TRACK_TYPE_VALUES) {
      if ((trackingTypeValue & trackType.value)  != 0) {
        // default value (in case samples were not updated since last emit)
        long value = 0;
        if (counts > 0) {
          // values for average, sum and count are not coming from sortedSamples
          switch (trackType) {
            case AVG:
              value = total / counts;
              break;
            case SUM:
              value = total;
              break;
            case COUNT:
              value = counts;
              break;
            default:
              value = sortedSamples[(int)((sortedSamples.length - 1) * trackType.indexInSamples)];
              break;
          }
        } else {
          // samples were not updated, i.e., no increment since the last
          // emit, so send a value of 0
        }
        stats.put(trackType, value);
      }
    }
    return stats;
  }
}
