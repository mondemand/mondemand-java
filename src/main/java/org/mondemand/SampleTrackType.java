package org.mondemand;

public enum SampleTrackType {

  MIN     (1 << 0, "_min", 0),
  MAX     (1 << 1, "_max", 1),
  AVG     (1 << 2, "_avg", 0),     // indexInSamples for avg is not applicable
  MEDIAN  (1 << 3, "_median", 0.5),
  PCTL_75 (1 << 4, "_pctl_75", 0.75),
  PCTL_90 (1 << 5, "_pctl_90", 0.90),
  PCTL_95 (1 << 6, "_pctl_95", 0.95),
  PCTL_98 (1 << 7, "_pctl_98", 0.98),
  PCTL_99 (1 << 8, "_pctl_99", 0.99);

  public final int      value;          // bitwise value for the enum
  public final String   keySuffix;      // suffix added to the stat's key
  public final double   indexInSamples; // index of the given enum in the
                                        // stat's samples

  /**
   * constructor for the enum
   * @param value - the bitwise value for the enum
   * @param keySuffix - the suffix added to the mondemand key
   * @param indexInSamples - index in the samples for the enum
   */
  SampleTrackType(int value, String keySuffix, double indexInSamples) {
    this.value = value;
    this.keySuffix = keySuffix;
    this.indexInSamples = indexInSamples;
  }

}
