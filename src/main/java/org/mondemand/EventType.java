package org.mondemand;

public enum EventType {
  LOG("MonDemand::LogMsg"),
  PERF("MonDemand::PerfMsg"),
  STATS("MonDemand::StatsMsg"),
  TRACE("MonDemand::TraceMsg");

  private final String eventName;

  private EventType(String eventName) {
    this.eventName = eventName;
  }

  public String getEventName() {
    return eventName;
  }
}
