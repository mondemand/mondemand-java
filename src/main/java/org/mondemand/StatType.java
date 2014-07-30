package org.mondemand;

public enum StatType {
  Unknown("unknown"),
  Gauge("gauge"),
  Counter("counter"),
  Timer("counter");         // timer itself is a counter, but extra stats we may
                            // emit, like min/max/avg/..., are gauges.

  public final String type;

  StatType (String type) {
    this.type = type;
  }

  public String toString() {
    return type;
  }
}
