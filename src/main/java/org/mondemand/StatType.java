package org.mondemand;

public enum StatType {
  Unknown("unknown"),
  Gauge("gauge"),
  Counter("counter");

  public final String type;

  StatType (String type) {
    this.type = type;
  }

  public String toString() {
    return type;
  }
}
