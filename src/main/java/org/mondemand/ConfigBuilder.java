package org.mondemand;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConfigBuilder {
  private static final String ADDR_FORMAT = "MONDEMAND_%sADDR";
  private static final String PORT_FORMAT = "MONDEMAND_%sPORT";
  private static final String TTL_FORMAT = "MONDEMAND_%sTTL";
  private static final String SENDTO_FORMAT = "MONDEMAND_%sSENDTO";
  private static final int TTL_MIN = 0;
  private static final int TTL_MAX = 32;

  public static Config buildDefaultConfig(Properties props) throws Exception {
    return buildEventSpecificConfig(props, null, null);
  }

  public static EventSpecificConfig buildEventSpecificConfig(Properties props,
                                                             EventType eventType,
                                                             Config defaults)
      throws Exception {
    List<InetAddress> addresses = getAddresses(props, eventType, defaults);
    List<Integer> ports = getPorts(props, eventType, defaults);
    List<Integer> ttls = getTtls(props, eventType, defaults);
    Integer sendTo = getSendTo(props, eventType, defaults);

    if (addresses == null || ports == null) {
      throw new Exception("ADDR and PORT must be specified");
    }

    if (ports.size() != 1 && ports.size() != addresses.size()) {
        throw new Exception("PORT list length should equal one or ADDR length");
    }

    if (ttls != null) {
      if (ttls.size() != 1 && ttls.size() != addresses.size()) {
        throw new Exception("TTL list length should equal one or ADDR length");
      }
    }

    if (sendTo != null && (sendTo < 1 || sendTo > addresses.size())) {
      throw new Exception("SENDTO value is outside the valid range of [1," +
                          addresses.size() + "]");
    }

    return new EventSpecificConfig(eventType, addresses, ports, ttls,sendTo);
  }

  private static String getConfigFragment(EventType eventType) {
    return (eventType == null ? "" : eventType.name());
  }

  private static String cleanString(String input) {
    if (input == null) {
      return null;
    }

    return input.replace("\"", "").replace(" ", "");
  }

  private static List<InetAddress> getAddresses(Properties props,
                                                EventType eventType,
                                                Config defaults)
      throws Exception {
    String addr_config = String.format(ADDR_FORMAT,
                                       getConfigFragment(eventType));

    if (props.getProperty(addr_config) == null) {
      if (defaults == null) {
        return null;
      } else {
        return defaults.getAddresses();
      }
    }

    String addr_str = cleanString(props.getProperty(addr_config));
    String[] addr_arr = addr_str.split(",");
    List<InetAddress> addresses = new ArrayList<InetAddress>();

    for (int i = 0; i < addr_arr.length; ++i) {
      addresses.add(InetAddress.getByName(addr_arr[i]));
    }

    return addresses;
  }

  private static List<Integer> getPorts(Properties props,
                                        EventType eventType,
                                        Config defaults) {
    String port_config = String.format(PORT_FORMAT,
                                       getConfigFragment(eventType));

    if (props.getProperty(port_config) == null) {
      if (defaults == null) {
        return null;
      } else {
        return defaults.getPorts();
      }
    }

    String port_str = cleanString(props.getProperty(port_config));
    String[] port_arr = port_str.split(",");
    List<Integer> ports = new ArrayList<Integer>();

    for (int i = 0; i < port_arr.length; ++i) {
      ports.add(Integer.parseInt(port_arr[i]));
    }

    return ports;
  }

  private static List<Integer> getTtls(Properties props,
                                       EventType eventType,
                                       Config defaults)
      throws Exception {
    String ttl_config = String.format(TTL_FORMAT,
                                      getConfigFragment(eventType));

    if (props.getProperty(ttl_config) == null) {
      if (defaults == null) {
        return null;
      } else {
        return defaults.getTtls();
      }
    }

    String ttl_str = cleanString(props.getProperty(ttl_config));
    String[] ttl_arr = ttl_str.split(",");
    List<Integer> ttls = new ArrayList<Integer>();

    for (int i = 0; i < ttl_arr.length; ++i) {
      int ttl = Integer.parseInt(ttl_arr[i]);

      if (ttl < TTL_MIN || ttl > TTL_MAX) {
        throw new Exception("TTL value specified in the config '" + ttl_config +
                            "' is outside the valid range of [" + TTL_MIN +
                            "," + TTL_MAX + "]");
      }

      ttls.add(Integer.parseInt(ttl_arr[i]));
    }

    return ttls;
  }

  private static Integer getSendTo(Properties props,
                                   EventType eventType,
                                   Config defaults) {
    String sendto_config = String.format(SENDTO_FORMAT,
                                         getConfigFragment(eventType));

    if (props.getProperty(sendto_config) == null) {
      if (defaults == null) {
        return null;
      } else {
        return defaults.getSendTo();
      }
    }

    return Integer.parseInt(cleanString(props.getProperty(sendto_config)));
  }
}
