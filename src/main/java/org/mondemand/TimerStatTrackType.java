package org.mondemand;

public enum TimerStatTrackType {

  MIN     (1 << 0, "min_", 0),
  MAX     (1 << 1, "max_", 1),
  AVG     (1 << 2, "avg_", 0),     // indexInSamples for avg has no meaning
  MEDIAN  (1 << 3, "median_", 0.5),
  PCTL_75 (1 << 4, "pctl_75_", 0.75),
  PCTL_90 (1 << 5, "pctl_90-", 0.90),
  PCTL_95 (1 << 6, "pctl_95_", 0.95),
  PCTL_98 (1 << 7, "pctl_98_", 0.98),
  PCTL_99 (1 << 8, "pctl_99_", 0.99);

  public final int      value;          // bitwise value for the enum
  public final String   keyPrefix;      // prefix added to the stat's key
  public final double   indexInSamples; // index of the given enum in the
                                        // stat's samples

  /**
   * constructor for the enum
   * @param value - the bitwise value for the enum
   * @param keyPrefix - the prefix added to the mondemand key
   * @param indexInSamples - index in the samples for the enum
   */
  TimerStatTrackType(int value, String keyPrefix, double indexInSamples) {
    this.value = value;
    this.keyPrefix = keyPrefix;
    this.indexInSamples = indexInSamples;
  }

}
