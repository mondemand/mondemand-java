package org.mondemand.tests;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.junit.Test;
import org.mondemand.Config;
import org.mondemand.ConfigBuilder;
import org.mondemand.EventSpecificConfig;
import org.mondemand.EventType;

public class ConfigBuilderTest {
  @Test
  public void testConfigBuilder() throws UnknownHostException {
    Properties props = new Properties();
    props.setProperty("MONDEMAND_ADDR", "127.0.0.1");
    props.setProperty("MONDEMAND_PORT", "9191");
    props.setProperty("MONDEMAND_PERF_ADDR", "127.0.0.2");
    props.setProperty("MONDEMAND_PERF_PORT", "9192");

    Config c = ConfigBuilder.buildDefaultConfig(props);

    assertEquals(c.getAddresses().size(), 1);
    assertEquals(c.getPorts().size(), 1);
    assertEquals(c.getAddresses().get(0), InetAddress.getByName("127.0.0.1"));
    assertEquals((int)c.getPorts().get(0), 9191);

    EventSpecificConfig esc1 =
      ConfigBuilder.buildEventSpecificConfig(props, EventType.PERF, c);

    assertEquals(esc1.getAddresses().size(), 1);
    assertEquals(esc1.getPorts().size(), 1);
    assertEquals(esc1.getAddresses().get(0), InetAddress.getByName("127.0.0.2"));
    assertEquals((int)esc1.getPorts().get(0), 9192);

    EventSpecificConfig esc2 =
      ConfigBuilder.buildEventSpecificConfig(props, EventType.STATS, c);

    assertEquals(esc2.getAddresses().size(), 1);
    assertEquals(esc2.getPorts().size(), 1);
    assertEquals(esc2.getAddresses().get(0), InetAddress.getByName("127.0.0.1"));
    assertEquals((int)esc2.getPorts().get(0), 9191);
  }
}
