package org.mondemand;

import java.net.InetAddress;
import java.util.List;
import java.util.Properties;

public class EventSpecificConfig extends Config {
  protected EventType eventType;

  public EventSpecificConfig(EventType eventType,
                             List<InetAddress> addresses,
                             List<InetAddress> interfaces,
                             List<Integer> ports,
                             List<Integer> ttls,
                             Integer sendTo) {
    super(addresses, interfaces, ports, ttls, sendTo);
    this.eventType = eventType;
  }

  public EventSpecificConfig(EventType eventType,
                             List<InetAddress> addresses,
                             List<Integer> ports,
                             List<Integer> ttls,
                             Integer sendTo) {
    this(eventType, addresses, null, ports, ttls, sendTo);
  }

  public EventSpecificConfig(EventType eventType,
                             List<InetAddress> addresses,
                             List<Integer> ports,
                             Integer sendTo) {
    this(eventType, addresses, ports, null, sendTo);
  }

  public EventSpecificConfig(EventType eventType,
                             List<InetAddress> addresses,
                             List<Integer> ports,
                             List<Integer> ttls) {
    this(eventType, addresses, ports, ttls, null);
  }

  public EventSpecificConfig(EventType eventType,
                             List<InetAddress> addresses,
                             List<Integer> ports) {
    this(eventType, addresses, ports, null, null);
  }

  public EventType getEventType() {
    return eventType;
  }

  public void setEventType(EventType eventType) {
    this.eventType = eventType;
  }

  public Properties toEmitterGroupProperties(EventType eventType) {
    String name = eventType.name();
    Properties props = super.toEmitterGroupProperties(name);

    if (eventType != null) {
      props.setProperty("lwes." + name + ".filter.type", "inclusion");
      props.setProperty("lwes." + name + ".filter.names",
                        eventType.getEventName());
    }

    return props;
  }
}
