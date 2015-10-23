package org.mondemand;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Config {
  public static final int TTL_DEFAULT = 5;
  protected List<InetAddress> addresses;
  protected List<InetAddress> interfaces;
  protected List<Integer> ports;
  protected List<Integer> ttls;
  protected Integer sendTo;

  public Config(List<InetAddress> addresses, List<InetAddress> interfaces,
                List<Integer> ports, List<Integer> ttls, Integer sendTo) {
    this.addresses = addresses;
    this.interfaces = interfaces;
    this.ports = ports;
    this.ttls = ttls;
    this.sendTo = sendTo;
  }

  public Config(List<InetAddress> addresses, List<Integer> ports,
                List<Integer> ttls, Integer sendTo) {
    this(addresses, null, ports, ttls, sendTo);
  }

  public Config(List<InetAddress> addresses, List<Integer> ports,
                Integer sendTo) {
    this(addresses, ports, null, sendTo);
  }

  public Config(List<InetAddress> addresses, List<Integer> ports,
                List<Integer> ttls) {
    this(addresses, ports, ttls, null);
  }

  public Config(List<InetAddress> addresses, List<Integer> ports) {
    this(addresses, ports, null, null);
  }

  public Config(InetAddress address, InetAddress networkInterface,
                Integer port, Integer ttl, Integer sendTo) {
    this(address == null ?
             null :
             Collections.<InetAddress>singletonList(address),
         networkInterface == null ?
             null :
             Collections.<InetAddress>singletonList(networkInterface),
         port == null ? null : Collections.<Integer>singletonList(port),
         ttl == null ? null : Collections.<Integer>singletonList(ttl),
         sendTo);

  }

  public List<InetAddress> getAddresses() {
    return addresses;
  }

  public void setAddresses(List<InetAddress> addresses) {
    this.addresses = addresses;
  }

  public List<InetAddress> getInterfaces() {
    return interfaces;
  }

  public void setInterfaces(List<InetAddress> interfaces) {
    this.interfaces = interfaces;
  }

  public List<Integer> getPorts() {
    return ports;
  }

  public void setPorts(List<Integer> ports) {
    this.ports = ports;
  }

  public List<Integer> getTtls() {
    return ttls;
  }

  public void setTtls(List<Integer> ttls) {
    this.ttls = ttls;
  }

  public Integer getSendTo() {
    return sendTo;
  }

  public void setSendTo(Integer sendTo) {
    this.sendTo = sendTo;
  }

  public Properties toEmitterGroupProperties(String emitterGroupName) {
    Properties emitterGroupProps = new Properties();
    String strategy = "all";
    StringBuilder hostsBuilder = new StringBuilder();
    String prefix = "";

    emitterGroupProps.setProperty("lwes.emitter_groups", emitterGroupName);

    for (int i = 0; i < getAddresses().size(); ++i) {
      hostsBuilder.append(prefix);

      if (getInterfaces() != null) {
        hostsBuilder.append(getInterfaces().get(i).getHostAddress());
        hostsBuilder.append(":");
      }

      InetAddress address = getAddresses().get(i);

      hostsBuilder.append(address.getHostAddress());
      hostsBuilder.append(":");

      if (getPorts().size() == 1) {
        hostsBuilder.append(getPorts().get(0));
      } else {
        hostsBuilder.append(getPorts().get(i));
      }

      if (address.isMulticastAddress()) {
        hostsBuilder.append(":");

        if (getTtls() == null) {
          hostsBuilder.append(TTL_DEFAULT);
        } else if (getTtls().size() == 1) {
          hostsBuilder.append(getTtls().get(0));
        } else {
          hostsBuilder.append(getTtls().get(i));
        }
      }

      prefix = ",";
    }

    emitterGroupProps.setProperty("lwes." + emitterGroupName + ".hosts",
                                  hostsBuilder.toString());

    if (getSendTo() != null) {
      strategy = sendTo + "ofN";
    }

    emitterGroupProps.setProperty("lwes." + emitterGroupName + ".strategy",
                                  strategy);

    return emitterGroupProps;
  }
}
